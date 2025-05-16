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


def run(query, params=None):
    return momapy_kb.neo4j.core.run(query, params=params)


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


def save_collections_from_entries(
    collection_names_and_entries,
    delete_all=False,
    check_connection=True,
):
    if check_connection:
        neo4j_dm.utils.check_connection()
    if delete_all:
        momapy_kb.neo4j.core.delete_all()
    collections = []
    for collection_name, collection_entries in collection_names_and_entries:
        collection = Collection(
            name=collection_name, entries=frozenset(collection_entries)
        )
        collections.append(collection)
    momapy_kb.neo4j.core.save_nodes_from_objects(
        collections,
        object_to_node_mode="hash",
    )


def save_collections_from_file_paths(
    collection_names_and_file_paths, delete_all=False, check_connection=True
):
    if check_connection:
        neo4j_dm.utils.check_connection()
    if delete_all:
        momapy_kb.neo4j.core.delete_all()
    collection_names_and_entries = []
    for (
        collection_name,
        collection_file_paths,
    ) in collection_names_and_file_paths:
        collection_entries = []
        for collection_file_path in collection_file_paths:
            result = momapy.io.read(collection_file_path, return_type="model")
            model_id, _ = os.path.splitext(
                os.path.basename(collection_file_path)
            )
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
                file_path=collection_file_path,
                rdf_annotations=annotations,
                ids=ids,
            )
            collection_entries.append(collection_entry)
        collection_names_and_entries.append(
            (
                collection_name,
                collection_entries,
            )
        )
    save_collections_from_entries(
        collection_names_and_entries,
        delete_all=delete_all,
        check_connection=check_connection,
    )
