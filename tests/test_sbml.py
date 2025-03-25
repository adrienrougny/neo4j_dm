import dataclasses

import frozendict

import momapy.core
import momapy.sbml.core
import momapy.sbml.io.sbml
import momapy.io
import momapy_kb.neo4j.core

import neo4j_dm.core
import credentials


@dataclasses.dataclass
class CollectionEntry(object):
    id_: str
    model: momapy.core.Model
    rdf_annotations: frozendict.frozendict[
        momapy.core.ModelElement, frozenset[momapy.sbml.core.RDFAnnotation]
    ]


# reader_result = momapy.io.read("test.sbml")
reader_result = momapy.io.read("recon2.xml")
model = reader_result.obj
rdf_annotations = reader_result.annotations
collection_entry = CollectionEntry(
    id_="recon2", model=model, rdf_annotations=rdf_annotations
)
momapy_kb.neo4j.core.connect(
    credentials.URI, credentials.USERNAME, credentials.PASSWORD
)
momapy_kb.neo4j.core.delete_all()
momapy_kb.neo4j.core.save_node_from_object(collection_entry)
