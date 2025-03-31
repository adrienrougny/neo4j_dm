import dataclasses
import os.path

import frozendict

import momapy.sbml.core
import momapy.core
import momapy_kb.neo4j.core
import momapy.celldesigner.io.celldesigner
import momapy.celldesigner.io.pickle
import momapy.io

import neo4j_dm.utils


def connect(hostname, username, password, protocol="bolt", port="7687"):
    momapy_kb.neo4j.core.connect(
        hostname=hostname,
        username=username,
        password=password,
        protocol=protocol,
        port=port,
    )


def close_connection():
    momapy_kb.neo4j.core.close_connection()


def is_connected():
    momapy_kb.neo4j.core.is_connected()


def delete_all():
    momapy_kb.neo4j.core.delete_all()


def run(query):
    return momapy_kb.neo4j.core.run(query)


@dataclasses.dataclass(frozen=True)
class CollectionEntry:
    id_: str
    model: momapy.core.Model
    rdf_annotations: (
        frozendict.frozendict[
            momapy.core.MapElement, frozenset[momapy.sbml.core.RDFAnnotation]
        ]
        | None
    ) = None
    file_path: str | None = None
    ids: frozendict.frozendict[momapy.core.MapElement, str] | None = None
    notes: frozendict.frozendict[momapy.core.MapElement, str] | None = None


@dataclasses.dataclass(frozen=True)
class Collection:
    name: str
    entries: frozenset[CollectionEntry] = dataclasses.field(
        default_factory=frozenset
    )


def save_collection_from_collection_entries(
    collection_name,
    collection_entries,
    delete_all=False,
    check_connection=True,
):
    if check_connection:
        neo4j_dm.utils.check_connection()
    if delete_all:
        momapy_kb.neo4j.core.delete_all()
    collection = Collection(
        name=collection_name, entries=frozenset(collection_entries)
    )
    momapy_kb.neo4j.core.save_node_from_object(
        collection,
        object_to_node_mode="hash",
    )


def save_collection_from_file_paths(
    collection_name, file_paths, delete_all=False, check_connection=True
):
    if check_connection:
        neo4j_dm.utils.check_connection()
    if delete_all:
        momapy_kb.neo4j.core.delete_all()
    collection_entries = []
    for file_path in file_paths:
        result = momapy.io.read(file_path, return_type="model")
        model_id, _ = os.path.splitext(os.path.basename(file_path))
        model = result.obj
        annotations = result.annotations
        annotations = frozendict.frozendict(
            {key: frozenset(val) for key, val in annotations.items()}
        )
        ids = result.ids
        ids = frozendict.frozendict(
            {key: frozenset(val) for key, val in ids.items()}
        )
        collection_entry = CollectionEntry(
            id_=model_id,
            model=model,
            file_path=file_path,
            rdf_annotations=annotations,
            ids=ids,
        )
        collection_entries.append(collection_entry)
    save_collection_from_collection_entries(
        collection_name,
        collection_entries,
        delete_all=delete_all,
        check_connection=check_connection,
    )
