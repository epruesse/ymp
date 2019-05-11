import asyncio
import atexit
import hashlib
import logging
import os
import re
import threading
from typing import List, Optional, Union
from urllib.parse import urlsplit

import aiohttp

from tqdm import tqdm

from ymp.common import ensure_list

LOG = logging.getLogger(__name__)


class FileDownloader(object):
    """Manages download of a set of URLs

    Downloads happen concurrently using asyncronous network IO.

    Args:
      block_size: Byte size of chunks to download
      timeout:    Aiohttp cumulative timeout
      parallel:   Number of files to download in parallel
      loglevel:   Log level for messages send to logging
                  (Errors are send with loglevel+10)
      alturls:    List of regexps modifying URLs
      retry:      Number of times to retry download
    """
    def __init__(self, block_size: int=4096, timeout: int=300, parallel: int=4,
                 loglevel: int=logging.WARNING, alturls=None, retry: int=3):
        self._block_size = block_size
        self._timeout = timeout
        self._parallel = parallel
        self._retry = retry

        self._alturls = []
        alturls = ["///"] + (alturls or [])
        for pat in alturls:
            sep = pat[0]
            if pat.strip(sep):
                patsub = re.split(r"(?<=[^\\])"+sep, pat.strip(sep))
                if len(patsub) != 2:
                    raise ValueError("Malformed regular expression '{}'"
                                     "".format(pat))
                patsub[1] = patsub[1].replace(r"\/", "/")
            else:
                patsub = ["", ""]
            self._alturls.append(patsub)

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # no loop in context (i.e. running in thread)
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self._sem = asyncio.Semaphore(parallel)
        self._progress = LOG.getEffectiveLevel() <= loglevel
        self._loglevel = loglevel
        self._sum_bar = None

    def log(self, msg: str, *args, modlvl: int=0, **kwargs) -> None:
        """Send message to logger

        Honors loglevel set for the FileDownloader object.

        Args:
          msg: The log message
          modlvl: Added to default logging level for object
        """
        LOG.log(self._loglevel + modlvl, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        """Send error to logger

        Message is sent with a log level 10 higher than the default
        for this object.
        """
        self.log(msg, *args, modlvl=10, **kwargs)

    @staticmethod
    def make_bar_format(desc_width: int=20, count_width: int=0,
                        rate: bool=False, eta: bool=False,
                        have_total: bool=True) -> str:
        """Construct bar_format for tqdm

        Args:
          desc_width: minimum space allocated for description
          count_width: min space for counts
          rate: show rate to right of progress bar
          eta: show eta to right of progress bar
          have_total: whether a total exists (required to add percentage)
        """
        if have_total:
            left = '{{desc:<{dw}}} {{percentage:3.0f}}%'.format(dw=desc_width)
        else:
            # percentage not supplied by TQDM if there is no total
            left = '{{desc:<{dw}}}'.format(dw=desc_width)
        right = ' {{n_fmt:>{cw}}} / {{total_fmt:<{cw}}}'.format(cw=count_width)
        if rate:
            right += ' {{rate_fmt:>{cw}}}'.format(cw=count_width+2)
        if eta:
            right += ' ETA {remaining}'
        return left + '|{bar}|' + right

    async def _download(self, session: aiohttp.ClientSession,
                        url: str, dest: str, md5: Optional[str]=None) -> bool:
        """Asynchronously download a single file

        - If ``dest`` points to an existing directory, the file name
        is derived from the trailing path portion of the URL.

        - Will skip download for existing files with matching MD5

        Args:
          session: aiohttp session object
          url:     source url
          dest:    destination path
          md5:     optional md5 checksum to verify
        """
        if os.path.isdir(dest):
            parts = urlsplit(url)
            basename = os.path.basename(parts.path)
            destfile = os.path.join(dest, basename)
        else:
            basename = os.path.basename(dest)
            destfile = dest

        if os.path.exists(destfile) and md5 and not isinstance(md5, bool):
            if self._check_md5(basename, destfile, md5):
                return True

        tryurls = [re.sub(pat, rep, url) for pat, rep in self._alturls]
        for url in tryurls:  # try alturls
            exc = None
            for num_try in range(self._retry):  # retry after timeout
                if exc:
                    self.log("Downloading %s failed with %s. Retrying %i/%i",
                             basename, exc, num_try, self._retry-1)
                try:
                    if await self._download_one(session, basename, url,
                                                destfile, md5):
                        return True
                    break
                except TimeoutError as e:
                    exc = e
        return False

    def _check_md5(self, name, fname, md5):
        md5_new = hashlib.md5()
        with open(fname, 'rb') as f:
            while True:
                block = f.read(8192)
                if not block:
                    break
                md5_new.update(block)
        if md5_new.hexdigest() == md5.strip():
            self.log("Download skipped: %s (file exists, md5 verified)", name)
            return True
        return False

    async def _download_one(self, session, name, url, dest, md5):
        part = dest+".part"
        if md5:
            md5_new = hashlib.md5()
        try:
            async with self._sem, \
                       session.get(url, timeout=self._timeout) as resp:
                if not resp.status == 200:
                    self.log("Download failed: %s (error code %i)",
                             name, resp.status)
                    self.log("  URL: '%s'", url.strip())
                    return False
                size = int(resp.headers.get('content-length', 0))

                if os.path.exists(dest):
                    existing_size = os.path.getsize(dest)
                    if existing_size == size:
                        if md5:
                            self.log("Overwriting: %s (md5 failed)", name)
                        else:
                            self.log("Download skipped: %s (file exists)",
                                     name)
                            return True
                    else:
                        self.log("Overwriting: %s (size mismatch %i!=%i)",
                                 name, size, existing_size)

                try:
                    self._sum_bar.total += size
                except AttributeError:
                    pass
                with open(part, mode="wb") as out, \
                    tqdm(total=size,
                         unit='B', unit_scale=True, unit_divisor=1024,
                         desc=name, leave=False,
                         miniters=1, disable=not self._progress,
                         bar_format=self.make_bar_format(40, 7, rate=True)) as t:
                    while True:
                        block = await resp.content.read(self._block_size)
                        if not block:
                            break
                        out.write(block)
                        if md5:
                            md5_new.update(block)
                        t.update(len(block))
                        self._sum_bar.update(len(block))
            os.rename(part, dest)
            if md5:
                md5_hash = md5_new.hexdigest()
                if isinstance(md5, bool):
                    self.log("Download complete: %s (md5=%s)", name,
                             md5_hash.strip())
                elif md5.strip() == md5_hash:
                    self.log("Download complete: %s (md5 verified)", name)
                else:
                    self.error("Download failed: %s (md5 failed)", name)
                    return False
            return True
        except (asyncio.CancelledError, asyncio.TimeoutError):
            if os.path.exists(part):
                os.unlink(part)
            raise

    async def _run(self, urls: List[str], dest: str,
                   md5s: Optional[List[str]]=None) -> List[bool]:
        """Executes a download session

        Args:
          urls: List of URLs
          dest: Destination path
          md5s: Optional list of md5 checksums
        """
        if not md5s:
            md5s = [None]*len(urls)
        async with aiohttp.ClientSession() as session:
            if len(urls) == 0:
                # No need to show progress bar for just 1 file
                self.log("Downloading 1 file.")
                result = await asyncio.ensure_future(
                    self._download(session, urls[0], dest, md5s[0])
                )
                self.log("Finished download.")
            else:
                self.log("Downloading %i files.", len(urls))
                coros = [
                    asyncio.ensure_future(
                        self._download(session, url, dest, md5)
                    )
                    for url, md5 in zip(urls, md5s)
                ]
                with tqdm(
                    asyncio.as_completed(coros), total=len(coros),
                    unit="Files", desc="Total files:",
                    disable=not self._progress, leave=False,
                    bar_format=self.make_bar_format(20, 7, eta=True)
                ) as t, tqdm(
                    total=1,  # must be >0
                    unit="B", desc="Total bytes:",
                    unit_scale=True, unit_divisor=1024,
                    disable=not self._progress, leave=False, miniters=1,
                    bar_format=self.make_bar_format(20, 7, rate=True)
                ) as t2:
                    self._sum_bar = t2
                    result = [await coro for coro in t]
                self.log("Finished downloads.")
        return result

    def get(self, urls: Union[str, List[str]], dest: str,
            md5s: Optional[List[str]]=None) -> None:
        """Download a list of URLs

        Args:
          urls: List of URLs
          dest: Destination folder
          md5s: List of MD5 sums to check
        """

        urls = ensure_list(urls)
        if not urls:
            return True  # nothing to do
        if len(urls) > 1:
            if not os.path.exists(dest):
                os.makedirs(dest)

        try:
            task = asyncio.ensure_future(self._run(urls, dest, md5s))
            self.loop.run_until_complete(task)
        except KeyboardInterrupt:
            end = asyncio.gather(*asyncio.Task.all_tasks())
            end.cancel()
            try:
                self.loop.run_until_complete(end)
            except asyncio.CancelledError:
                pass
            raise

        return all(task.result())


class DownloadThread(object):
    def __init__(self):
        LOG.error("made downloader")
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.main)
        self.thread.start()
        atexit.register(self.terminate)

    def terminate(self):
        self.loop.call_soon_threadsafe(self.loop.stop)

    def main(self):
        LOG.error("here")
        asyncio.set_event_loop(self.loop)
        self.downloader = FileDownloader()
        self.loop.run_forever()

    def get(self, url, dest, md5):
        LOG.error("scheduling get %s", url)
        self.loop.call_soon_threadsafe(
            self.downloader.get(url, dest, md5)
        )


#DOWNLOADER = DownloadThread()

#def download(url, dest, md5=None):
#    LOG.error("called download %s", url)
#    DOWNLOADER.get(url, dest, md5)
