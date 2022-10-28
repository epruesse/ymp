import logging
import os

import pytest

import ymp
from ymp import yaml
from ymp.stage import Reference, StageStack
from ymp.stage.reference import Resource
from ymp.exceptions import YmpConfigError

references_test = "references/test"  # place sonarcloud


def make_cfg(text, *args):
    fname = "test.yml"
    with open(fname, "w") as f:
        f.write("\n".join(["ref:"] + [" " + a for a in text.splitlines() + list(args)]))
    cfg = yaml.load([fname])
    return cfg["ref"]


@pytest.fixture()
def check_show(capsys):
    def checker(exc, substr):
        exc.show()
        log = capsys.readouterr()
        assert substr in log.err
        assert not log.out

    return checker


def test_not_list(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("asd:"))
    assert excinfo.match("must be list")
    check_show(excinfo.value, "line 2")


def test_empty_ref(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("-"))
    assert excinfo.match("Empty")
    check_show(excinfo.value, "line 2")


def test_empty_unknown_type(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("- type: mountain"))
    assert excinfo.match("Unknown type")
    assert excinfo.match("mountain")
    check_show(excinfo.value, "line 2")


def test_fasta_no_url(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("- type: fasta"))
    assert excinfo.match("fasta")
    assert excinfo.match("must have 'url'")
    check_show(excinfo.value, "line 2")


def test_fasta_with_url(saved_cwd, check_show):
    ref = Reference("test", make_cfg("- type: fasta", "  url: somewhere"))


def test_duplicate_resource(saved_cwd):
    from ymp.stage.reference import FileResource

    with pytest.raises(ValueError) as excinfo:
        class duplicate(FileResource):
            pass

    assert excinfo.match("'file'")
    assert excinfo.match("duplicate type")


def test_resource_not_mapping(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("- []"))
    assert excinfo.match("mapping")
    check_show(excinfo.value, "line 2")


def test_resource_not_mapping_third(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference(
            "test",
            make_cfg(
                "- type: fasta",
                "  url: somewhere",
                "- type: fasta",
                "  url: somewhere",
                "- []",
            ),
        )
    assert excinfo.match("mapping")
    check_show(excinfo.value, "line 6")


def test_get_id_name(saved_cwd):
    ref = Reference(
        "test", make_cfg("- type: fasta", "  id: customid", "  url: somewhere")
    )
    # FIXME, check IDs in reference, this just triggers resource


def test_file_resource_no_extension(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg("- type: file", "  url: somewhere"))
    assert excinfo.match("must have")
    assert excinfo.match("extension")
    check_show(excinfo.value, "line 2")


def test_file_resource(saved_cwd):
    ref = Reference(
        "test", make_cfg("- type: file", "  url: somewhere", "  extension: bam")
    )


def test_named_unpacked_resource(saved_cwd):
    ref = Reference("test", make_cfg("- type: gtf", "  url: somewhere"))
    assert ref.files == {"ALL.gtf": "somewhere"}


def test_archive_resource_no_url(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg(" - type: archive"))
    assert excinfo.match("must have")
    assert excinfo.match("url")
    check_show(excinfo.value, "line 2")


def test_archive_resource_no_files(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference("test", make_cfg(" - type: archive", "   url: somwhere"))
    assert excinfo.match("must have")
    assert excinfo.match("files")
    check_show(excinfo.value, "line 2")


def test_archive_resource_files_not_mapping(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        Reference(
            "test", make_cfg(" - type: archive", "   url: somwhere", "   files:")
        )
    assert excinfo.match("must be mapping")
    assert excinfo.match("files")
    check_show(excinfo.value, "line 4")


def test_archive_resource_no_url(saved_cwd, check_show):
    ref = Reference(
        "test",
        make_cfg(
            " - type: archive",
            "   url: somwhere",
            "   files:",
            "     ALL.bam: some.bam",
        ),
    )
    assert list(ref.files.keys()) == ["ALL.bam"]
    assert ref.files["ALL.bam"].endswith("/some.bam")
    assert ref.files["ALL.bam"].startswith(references_test)


def test_localdir_resource_no_files(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference("test", make_cfg(" - type: localdir", "   url: somwhere"))
    assert excinfo.match("must have")
    assert excinfo.match("files")
    check_show(excinfo.value, "line 2")


def test_localdir_resource_files_not_mapping(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test", make_cfg(" - type: localdir", "   url: somwhere", "   files:")
        )
    assert excinfo.match("must be mapping")
    assert excinfo.match("files")
    check_show(excinfo.value, "line 4")


def test_localdir_resource(saved_cwd):
    ref = Reference(
        "test",
        make_cfg(
            " - type: localdir",
            "   url: somewhere",
            "   files:",
            "     ALL.bam: some.bam",
        ),
    )


def test_regexlocaldir_directory_missing(saved_cwd, check_show):
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(
                " - type: path",
                "   url: somewhere",
                "   match: [something]",
            ),
        )
    assert excinfo.match("Directory")
    assert excinfo.match("somewhere")
    check_show(excinfo.value, "line 2")


def test_regexlocaldir_no_match(saved_cwd, check_show):
    os.mkdir("somewhere")
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(
                " - type: path",
                "   url: somewhere",
            ),
        )
    assert excinfo.match("must have")
    assert excinfo.match("match")
    check_show(excinfo.value, "line 2")


def test_regexlocaldir_match_not_list(saved_cwd, check_show):
    os.mkdir("somewhere")
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(" - type: path", "   url: somewhere", "   match: something"),
        )
    assert excinfo.match("must be")
    assert excinfo.match("match")
    check_show(excinfo.value, "line 4")


def test_regexlocaldir_match_no_files(saved_cwd, check_show):
    os.mkdir("somewhere")
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(" - type: path", "   url: somewhere", "   match: [(?P<sample>)]"),
        )
    assert excinfo.match("no files")
    check_show(excinfo.value, "line 2")


def test_regexlocaldir_match_broken_regex(saved_cwd, check_show):
    os.mkdir("somewhere")
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(" - type: path", "   url: somewhere", "   match: [(?P<sample>]"),
        )
    assert excinfo.match("compile")
    assert excinfo.match("missing \)")
    check_show(excinfo.value, "line 4")


def test_regexlocaldir_match_regex_no_sample(saved_cwd, check_show):
    os.mkdir("somewhere")
    with pytest.raises(YmpConfigError) as excinfo:
        ref = Reference(
            "test",
            make_cfg(
                " - type: path",
                "   url: somewhere",
                "   match:",
                "   - (?P<sample>.)",
                "   - (?P<sample1>.)",
            ),
        )
    assert excinfo.match("must have")
    assert excinfo.match("sample")
    check_show(excinfo.value, "line 6")


def test_regexlocaldir_resource(saved_cwd):
    os.mkdir("somewhere")
    open("somewhere/test.file", "a").close()
    ref = Reference(
        "test",
        make_cfg(
            " - type: path",
            "   url: somewhere",
            "   match:",
            "   - (?P<sample>[^.]*)\.file",
        ),
    )


def test_get_path(demo_dir):
    ref = Reference(
        "test",
        make_cfg(
            "- type: fasta",
            "  url: somewhere",
        ),
    )
    assert ref.get_path(None) == references_test
    ## FIXME: Do we need the below feature at all?9
    assert str(ref) == "references/test/ALL"


def test_get_all_targets(demo_dir):
    ref = Reference(
        "test",
        make_cfg(
            "- type: fasta",
            "  url: somewhere",
        ),
    )
    assert ref.get_all_targets(None) == ["references/test/ALL.fasta.gz"]


def test_no_ids(demo_dir):
    ref = Reference("test", make_cfg("- type: fasta", "  url: somewhere"))
    stack = StageStack("toy")
    groups = ref.get_group(stack, ["bla"])
    assert groups == []
    ids = ref.get_ids(stack, groups)
    assert ids == ["ALL"]


def test_with_ids(demo_dir):
    ref = Reference(
        "test",
        make_cfg(
            "- type: fasta",
            "  url: somewhere/1.fasta",
            "  id: one",
            "- type: fasta",
            "  url: elsewhere/2.fasta",
            "  id: two",
        ),
    )
    stack = StageStack("toy")
    groups = ref.get_group(stack, ["bla"])
    assert groups == ["ref_test"]
    ids = ref.get_ids(stack, groups)
    assert set(ids) == set(["one", "two"])
    assert ref.outputs == {"/{sample}.fasta.gz": ""}


def test_duplicate_file(saved_cwd, check_show):
    ref = Reference(
        "test",
        make_cfg(
            "- type: fasta", "  url: somewhere", "- type: fasta", "  url: somewhere"
        ),
    )
    with pytest.raises(YmpConfigError) as excinfo:
        ref.files
    assert excinfo.match("Duplicate")
    check_show(excinfo.value, "line 4")


def test_get_file(saved_cwd):
    ref = Reference("test", make_cfg("- type: fasta", "  url: somewhere.fasta.gz"))
    assert ref.get_file("ALL.fasta.gz") == "somewhere.fasta.gz"
    assert ref.get_file("ALL.fasta.gz", isdir=True) == \
        "YMP ERROR: File 'somewhere.fasta.gz' should be directory but is not"
    assert ref.get_file("blabla").startswith("YMP_FILE_NOT_FOUND")


def test_add_rule(saved_cwd):
    ref = Reference("test", make_cfg("- type: fasta", "  url: somewhere.fasta.gz"))
    assert ref.prev() == references_test
    assert ref.get_file("ALL.sometype").startswith("YMP_FILE_NOT_FOUND")
    kwargs = {"item": "{:this:}/{:target:}.sometype"}
    assert ref.this(kwargs=kwargs) == references_test
    assert ref.get_file("ALL.sometype").startswith("YMP_FILE_NOT_FOUND")
    kwargs["field"] = "output"
    ref.set_active(ref)
    assert ref.this(kwargs=kwargs) == references_test
    assert ref.get_file("{sample}.sometype") == references_test + "/{sample}.sometype"
