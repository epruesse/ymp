import logging

import pytest
import pygraphviz as pgv
import networkx as nx


def test_pipeline_hide(invoker):
    """Checks that hiding of pipeline intermediary outputs works"""
    
    invoker.call("init", "demo")
    res = invoker.call("make", "toy.mypipeline", "--dag", "-qq")
    
    # This line will segfault if there is any extra data in res!
    dotgraph = pgv.AGraph(res.output)

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
   
    
