import logging

import pytest
import pygraphviz as pgv
import networkx as nx

import ymp
from ymp.stage import Pipeline, StageStack
from ymp import yaml
from ymp.exceptions import YmpConfigError


def test_pipeline_hide(invoker, demo_dir):
    """Checks that hiding of pipeline intermediary outputs works"""
    
    res = invoker.call("make", "toy.mypipeline", "--dag", "-qq")
    # Graphvis is really fragile w.r.t. input graph format. We need to
    # make sure it gets fed the graph and only the graph, otherwise it
    # will segfault on us.
    # The graph starts with "digraph". Make sure we have that
    assert "digraph" in res.output
    # Cut of anything before. Keeping snakemake quiet is just too
    # fragile. Something always talks, so we just cut that off to make
    # testing robust.
    graphtext = res.output[res.output.index("digraph"):]
    # The last line minus white space must comprise a "}" ending the graph
    assert graphtext.splitlines()[-1].strip() == "}"
    # Findgers crossed...
    dotgraph = pgv.AGraph(graphtext)

    graph = nx.DiGraph(dotgraph)
    nodemap = {
        label.split(r"\n")[0]: node
        for node, label in graph.nodes(data="label")
    }
    before_mapping = set(graph.predecessors(nodemap['bowtie2_map']))
    before_assembly = set(graph.predecessors(nodemap['megahit']))

    # Assert that assembly draws reads from dust, not trim stage
    assert nodemap['bbmap_trim'] not in before_assembly
    assert nodemap['bbmap_dust'] in before_assembly
    # Assert that mapping draws reads from trim, not dust
    assert nodemap['bbmap_dust'] not in before_mapping
    assert nodemap['bbmap_trim'] in before_mapping

def make_cfg(text):
    fname = "test.yml"
    with open(fname, "w") as f:
        f.write(text)
    cfg = yaml.load([fname])
    return cfg


def test_params_must_be_mapping(saved_cwd):
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages: [trim_bbmap]\n"
            "params: wrong"
        ))
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages: [trim_bbmap]\n"
            "params: [wrong]"
        ))
    Pipeline("test", make_cfg(
        "stages: [trim_bbmap]\n"
        "params:"
    ))

def test_parameter_must_be_mapping(saved_cwd):
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages: [trim_bbmap]\n"
            "params:\n"
            "  someparam:"
        ))
    Pipeline("test", make_cfg(
        "stages: [trim_bbmap]\n"
        "params:\n"
        "  someparam:\n"
        "    key: A\n"
        "    type: int\n"
        "    default: 1"
    ))

def test_param_must_have_key_and_type(saved_cwd):
    Pipeline("test", make_cfg(
        "stages: [trim_bbmap]\n"
        "params:\n"
        "  someparam:\n"
        "    key: A\n"
        "    type: int\n"
        "    default: 1"
    ))
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages: [trim_bbmap]\n"
            "params:\n"
            "  someparam:\n"
            "    type: int\n"
            "    default: 1"
    ))
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages: [trim_bbmap]\n"
            "params:\n"
            "  someparam:\n"
            "    key: A\n"
            "    default: 1"
    ))
    
def test_pipeline_must_have_stages(saved_cwd):
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "hide: False"
        ))

def test_pipeline_stage_name_must_not_be_empty(saved_cwd):
    with pytest.raises(YmpConfigError):
        Pipeline("test", make_cfg(
            "stages:\n"
            " -"
        ))
  
def test_pipeline_stage_maybe_dict(saved_cwd):
    Pipeline("test", make_cfg(
        "stages:\n"
        " - trim_bbmap\n"
        " - trim_bbmap:\n"
        " - trim_bbmap\n"
        " - trim_bbmap:\n"
    ))
 
def test_param_from_stage(saved_cwd):
    pipe = Pipeline("test", make_cfg(
        "stages: [trim_bbmap]"
    ))
    assert pipe.params

def test_stage_with_curly(saved_cwd):
    pipe = Pipeline("test", make_cfg(
        "params:\n"
        "  tool:\n"
        "    key: _\n"
        "    type: choice\n"
        "    default: bbmap\n"
        "    value: [bbmap, sickle]\n"
        "stages:\n"
        " - trim_{tool}"
    ))
    assert pipe.params
 
def test_stage_not_parametrizable(saved_cwd):
    pipe = Pipeline("test", make_cfg(
        "stages:\n"
        " - ref_phiX\n"
    ))
    assert pipe.params == [] 


class mock_stack:
    def __init__(self, name):
        self.name = f"stack.{name}"
        self.stage_name = name
        self.stage = f"stage.{name}"

    
def test_pipeline_path(saved_cwd):
    stack = mock_stack("test_pipe")
    pipe = Pipeline("test_pipe", make_cfg(
        "stages:\n"
        " - trim_bbmap\n"
        " - ref_phiX"
    ))
    assert pipe.get_path(stack) == "stack.trim_bbmap.ref_phiX"
    assert pipe.get_path(stack, "/{sample}.R1.fq.gz") == "stack.trim_bbmap"
    assert pipe.get_path(stack, "/{sample}.fasta.gz") == "stack.trim_bbmap.ref_phiX"


def test_pipeline_path_with_param(saved_cwd):
    stack = mock_stack("test_pipe")
    pipe = Pipeline("test_pipe", make_cfg(
        "stages:\n"
        " - trim_bbmapQ10\n"
        " - ref_phiX"
    ))
    assert pipe.get_path(stack) == "stack.trim_bbmapQ10.ref_phiX"
    assert pipe.get_path(stack, "/{sample}.R1.fq.gz") == "stack.trim_bbmapQ10"
    assert pipe.get_path(stack, "/{sample}.fasta.gz") == "stack.trim_bbmapQ10.ref_phiX"


def test_pipeline_can_provide(saved_cwd):
    pipe = Pipeline("test_pipe", make_cfg(
        "stages:\n"
        " - trim_bbmap\n"
        " - ref_phiX"
    ))
    assert pipe.can_provide(set((
        "/{sample}.R1.fq.gz",
    ))) == {
        "/{sample}.R1.fq.gz": ".trim_bbmap"
    }

def test_get_all_targets(demo_dir):
    with open("ymp.yml", "a") as f:
        f.write("include: test.yml")
    with open("test.yml", "w") as f:
        f.write(
            "pipelines:\n"
            " test_pipe:\n"
            "   stages:\n"
            "    - trim_bbmap\n"
            "    - ref_phiX\n"
        )
    ymp.get_config().unload()
    pipe = ymp.get_config().pipelines.test_pipe
    stack = StageStack("toy.test_pipe")
    assert pipe.get_all_targets(stack) == ["toy.test_pipe", "references/phiX/ALL.fasta.gz"]
    assert pipe.get_ids(stack, []) == ["ALL"]
 
