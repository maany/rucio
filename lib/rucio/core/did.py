# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import random
from datetime import datetime, timedelta
from hashlib import md5
from re import match
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from sqlalchemy import and_, delete, exists, insert, or_, update
from sqlalchemy.exc import DatabaseError, IntegrityError, NoResultFound
from sqlalchemy.sql import func, not_
from sqlalchemy.sql.expression import bindparam, case, false, null, select, true

import rucio.core.replica  # import add_replicas
import rucio.core.rule
from rucio.common import exception
from rucio.common.config import config_get_bool, config_get_int
from rucio.common.constants import DEFAULT_VO
from rucio.common.utils import chunks, is_archive
from rucio.core import did_meta_plugins
from rucio.core.message import add_message
from rucio.core.monitor import MetricManager
from rucio.core.naming_convention import validate_name
from rucio.db.sqla import filter_thread_work, models
from rucio.db.sqla.constants import BadFilesStatus, DIDAvailability, DIDReEvaluation, DIDType, RuleState
from rucio.db.sqla.session import read_session, stream_session, transactional_session
from rucio.db.sqla.util import temp_table_mngr

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from sqlalchemy.orm import Session
    from sqlalchemy.sql._typing import ColumnExpressionArgument
    from sqlalchemy.sql.selectable import Select

    from rucio.common.types import InternalAccount, InternalScope, LoggerFunction


METRICS = MetricManager(module=__name__)


@read_session
def list_expired_dids(
        worker_number: Optional[int] = None,
        total_workers: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        session: "Session"
) -> list[dict[str, Any]]:
    """
    List expired data identifiers.

    :param limit: limit number.
    :param session: The database session in use.
    """

    sub_query = exists(
    ).where(
        models.ReplicationRule.scope == models.DataIdentifier.scope,
        models.ReplicationRule.name == models.DataIdentifier.name,
        models.ReplicationRule.locked == true(),
    )
    list_stmt = select(
        models.DataIdentifier.scope,
        models.DataIdentifier.name,
        models.DataIdentifier.did_type,
        models.DataIdentifier.created_at,
        models.DataIdentifier.purge_replicas
    ).with_hint(
        models.DataIdentifier,
        'INDEX(DIDS DIDS_EXPIRED_AT_IDX)',
        'oracle'
    ).where(
        models.DataIdentifier.expired_at < datetime.utcnow(),
        not_(sub_query),
    ).order_by(
        models.DataIdentifier.expired_at
    )

    if session.bind.dialect.name in ['oracle', 'mysql', 'postgresql']:
        list_stmt = filter_thread_work(session=session, query=list_stmt, total_threads=total_workers, thread_id=worker_number, hash_variable='name')
    elif session.bind.dialect.name == 'sqlite' and worker_number and total_workers and total_workers > 0:
        row_count = 0
        dids = list()
        for scope, name, did_type, created_at, purge_replicas in session.execute(list_stmt).yield_per(10):
            if int(md5(name).hexdigest(), 16) % total_workers == worker_number:
                dids.append({'scope': scope,
                             'name': name,
                             'did_type': did_type,
                             'created_at': created_at,
                             'purge_replicas': purge_replicas})
                row_count += 1
            if limit and row_count >= limit:
                return dids
        return dids
    else:
        if worker_number and total_workers:
            raise exception.DatabaseException('The database type %s returned by SQLAlchemy is invalid.' % session.bind.dialect.name)

    if limit:
        list_stmt = list_stmt.limit(limit)

    return [{'scope': scope, 'name': name, 'did_type': did_type, 'created_at': created_at,
             'purge_replicas': purge_replicas} for scope, name, did_type, created_at, purge_replicas in session.execute(list_stmt)]


@transactional_session
def add_did(
        scope: "InternalScope",
        name: str,
        did_type: Union[str, DIDType],
        account: "InternalAccount",
        statuses: Optional["Mapping[str, Any]"] = None,
        meta: Optional["Mapping[str, Any]"] = None,
        rules: Optional["Sequence[str]"] = None,
        lifetime: Optional[int] = None,
        dids: Optional["Sequence[Mapping[str, Any]]"] = None,
        rse_id: Optional[str] = None,
        *,
        session: "Session",
) -> None:
    """
    Add data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param did_type: The data identifier type.
    :param account: The account owner.
    :param statuses: Dictionary with statuses, e.g.g {'monotonic':True}.
    :meta: Meta-data associated with the data identifier is represented using key/value pairs in a dictionary.
    :rules: Replication rules associated with the data identifier. A list of dictionaries, e.g., [{'copies': 2, 'rse_expression': 'TIERS1'}, ].
    :param lifetime: DID's lifetime (in seconds).
    :param dids: The content.
    :param rse_id: The RSE id when registering replicas.
    :param session: The database session in use.
    """
    return add_dids(dids=[{'scope': scope, 'name': name, 'type': did_type,
                           'statuses': statuses or {}, 'meta': meta or {},
                           'rules': rules, 'lifetime': lifetime,
                           'dids': dids, 'rse_id': rse_id}],
                    account=account, session=session)


@transactional_session
def add_dids(
        dids: "Sequence[dict[str, Any]]",
        account: "InternalAccount",
        *,
        session: "Session",
) -> None:
    """
    Bulk add data identifiers.

    :param dids: A list of DIDs.
    :param account: The account owner.
    :param session: The database session in use.
    """
    try:

        for did in dids:
            try:

                if isinstance(did['type'], str):
                    did['type'] = DIDType[did['type']]

                if did['type'] == DIDType.FILE:
                    raise exception.UnsupportedOperation('Only collection (dataset/container) can be registered.')

                # Lifetime
                expired_at = None
                if did.get('lifetime'):
                    expired_at = datetime.utcnow() + timedelta(seconds=did['lifetime'])

                # Insert new data identifier
                new_did = models.DataIdentifier(scope=did['scope'], name=did['name'], account=did.get('account') or account,
                                                did_type=did['type'], monotonic=did.get('statuses', {}).get('monotonic', False),
                                                is_open=True, expired_at=expired_at)

                new_did.save(session=session, flush=False)

                if 'meta' in did and did['meta']:
                    # Add metadata
                    set_metadata_bulk(scope=did['scope'], name=did['name'], meta=did['meta'], recursive=False, session=session)

                if did.get('dids', None):
                    attach_dids(scope=did['scope'], name=did['name'], dids=did['dids'],
                                account=account, rse_id=did.get('rse_id'), session=session)

                if did.get('rules', None):
                    rucio.core.rule.add_rules(dids=[did, ], rules=did['rules'], session=session)

                event_type = None
                if did['type'] == DIDType.CONTAINER:
                    event_type = 'CREATE_CNT'
                if did['type'] == DIDType.DATASET:
                    event_type = 'CREATE_DTS'
                if event_type:
                    message = {'account': account.external,
                               'scope': did['scope'].external,
                               'name': did['name'],
                               'expired_at': str(expired_at) if expired_at is not None else None}
                    if account.vo != DEFAULT_VO:
                        message['vo'] = account.vo

                    add_message(event_type, message, session=session)

            except KeyError:
                # ToDo
                raise

        session.flush()

    except IntegrityError as error:
        if match('.*IntegrityError.*ORA-00001: unique constraint.*DIDS_PK.*violated.*', error.args[0]) \
                or match('.*IntegrityError.*UNIQUE constraint failed: dids.scope, dids.name.*', error.args[0]) \
                or match('.*IntegrityError.*1062.*Duplicate entry.*for key.*', error.args[0]) \
                or match('.*IntegrityError.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*UniqueViolation.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*IntegrityError.*columns? .*not unique.*', error.args[0]):
            raise exception.DataIdentifierAlreadyExists('Data Identifier already exists!')

        if match('.*IntegrityError.*02291.*integrity constraint.*DIDS_SCOPE_FK.*violated - parent key not found.*', error.args[0]) \
                or match('.*IntegrityError.*FOREIGN KEY constraint failed.*', error.args[0]) \
                or match('.*IntegrityError.*1452.*Cannot add or update a child row: a foreign key constraint fails.*', error.args[0]) \
                or match('.*IntegrityError.*02291.*integrity constraint.*DIDS_SCOPE_FK.*violated - parent key not found.*', error.args[0]) \
                or match('.*IntegrityError.*insert or update on table.*violates foreign key constraint.*', error.args[0]) \
                or match('.*ForeignKeyViolation.*insert or update on table.*violates foreign key constraint.*', error.args[0]) \
                or match('.*IntegrityError.*foreign key constraints? failed.*', error.args[0]):
            raise exception.ScopeNotFound('Scope not found!')

        raise exception.RucioException(error.args)
    except DatabaseError as error:
        if match('.*(DatabaseError).*ORA-14400.*inserted partition key does not map to any partition.*', error.args[0]):
            raise exception.ScopeNotFound('Scope not found!')
        raise exception.RucioException(error.args)


@transactional_session
def attach_dids(
        scope: "InternalScope",
        name: str,
        dids: "Sequence[Mapping[str, Any]]",
        account: "InternalAccount",
        ignore_duplicate: bool = False,
        rse_id: Optional[str] = None,
        *,
        session: "Session",
) -> None:
    """
    Append data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param dids: The content.
    :param account: The account owner.
    :param ignore_duplicate: If True, ignore duplicate entries.
    :param rse_id: The RSE id for the replicas.
    :param session: The database session in use.
    """
    return attach_dids_to_dids(attachments=[{'scope': scope, 'name': name, 'dids': dids, 'rse_id': rse_id}], account=account, ignore_duplicate=ignore_duplicate, session=session)


@transactional_session
def attach_dids_to_dids(
        attachments: "Sequence[Mapping[str, Any]]",
        account: "InternalAccount",
        ignore_duplicate: bool = False,
        *,
        session: "Session",
) -> None:
    children_temp_table = temp_table_mngr(session).create_scope_name_table()
    parent_dids = list()
    first_iteration = True
    for attachment in attachments:
        try:
            children = {(a['scope'], a['name']): a for a in attachment['dids']}
            cont = []
            stmt = select(
                models.DataIdentifier
            ).with_hint(
                models.DataIdentifier,
                'INDEX(DIDS DIDS_PK)',
                'oracle'
            ).where(
                models.DataIdentifier.scope == attachment['scope'],
                models.DataIdentifier.name == attachment['name']
            )
            parent_did = session.execute(stmt).scalar_one()
            update_parent = False

            if not first_iteration:
                stmt = delete(
                    children_temp_table
                )
                session.execute(stmt)
            values = [{'scope': s, 'name': n} for s, n in children]
            stmt = insert(
                children_temp_table
            )
            session.execute(stmt, values)

            if parent_did.did_type == DIDType.FILE:
                # check if parent file has the archive extension
                if is_archive(attachment['name']):
                    __add_files_to_archive(parent_did=parent_did,
                                           files_temp_table=children_temp_table,
                                           files=children,
                                           account=account,
                                           ignore_duplicate=ignore_duplicate,
                                           session=session)
                    return
                raise exception.UnsupportedOperation("Data identifier '%(scope)s:%(name)s' is a file" % attachment)

            elif not parent_did.is_open:
                raise exception.UnsupportedOperation("Data identifier '%(scope)s:%(name)s' is closed" % attachment)

            elif parent_did.did_type == DIDType.DATASET:
                cont = __add_files_to_dataset(parent_did=parent_did,
                                              files_temp_table=children_temp_table,
                                              files=children,
                                              account=account,
                                              ignore_duplicate=ignore_duplicate,
                                              rse_id=attachment.get('rse_id'),
                                              session=session)
                update_parent = len(cont) > 0

            elif parent_did.did_type == DIDType.CONTAINER:
                __add_collections_to_container(parent_did=parent_did,
                                               collections_temp_table=children_temp_table,
                                               collections=children,
                                               account=account,
                                               ignore_duplicate=ignore_duplicate,
                                               session=session)
                update_parent = True

            if update_parent:
                # cont contains the parent of the files and is only filled if the files does not exist yet
                parent_dids.append({'scope': parent_did.scope,
                                    'name': parent_did.name,
                                    'rule_evaluation_action': DIDReEvaluation.ATTACH})
        except NoResultFound:
            raise exception.DataIdentifierNotFound("Data identifier '%s:%s' not found" % (attachment['scope'], attachment['name']))
        first_iteration = False

    # Remove all duplicated dictionaries from the list
    # (convert the list of dictionaries into a list of tuple, then to a set of tuple
    # to remove duplicates, then back to a list of unique dictionaries)
    parent_dids = [dict(tup) for tup in set(tuple(dictionary.items()) for dictionary in parent_dids)]
    if parent_dids:
        stmt = insert(
            models.UpdatedDID
        )
        session.execute(stmt, parent_dids)


def __add_files_to_archive(
        parent_did: models.DataIdentifier,
        files_temp_table: Any,
        files: "Mapping[tuple[InternalScope, str], Mapping[str, Any]]",
        account: "InternalAccount",
        ignore_duplicate: bool = False,
        *,
        session: "Session"
) -> None:
    """
    Add files to the archive.

    :param parent_did: the DataIdentifier object of the parent did
    :param files: archive content.
    :param account: The account owner.
    :param ignore_duplicate: If True, ignore duplicate entries.
    :param session: The database session in use.
    """
    stmt = select(
        files_temp_table.scope,
        files_temp_table.name,
        models.DataIdentifier.scope.label('did_scope'),
        models.DataIdentifier.bytes,
        models.DataIdentifier.guid,
        models.DataIdentifier.events,
        models.DataIdentifier.availability,
        models.DataIdentifier.adler32,
        models.DataIdentifier.md5,
        models.DataIdentifier.is_archive,
        models.DataIdentifier.constituent,
        models.DataIdentifier.did_type,
    ).outerjoin_from(
        files_temp_table,
        models.DataIdentifier,
        and_(models.DataIdentifier.scope == files_temp_table.scope,
             models.DataIdentifier.name == files_temp_table.name)
    )
    if ignore_duplicate:
        stmt = stmt.add_columns(
            models.ConstituentAssociation.scope.label('archive_contents_scope'),
        ).outerjoin_from(
            files_temp_table,
            models.ConstituentAssociation,
            and_(models.ConstituentAssociation.scope == parent_did.scope,
                 models.ConstituentAssociation.name == parent_did.name,
                 models.ConstituentAssociation.child_scope == files_temp_table.scope,
                 models.ConstituentAssociation.child_name == files_temp_table.name)
        )

    dids_to_add = {}
    must_set_constituent = False
    archive_contents_to_add = {}
    for row in session.execute(stmt):
        file = files[row.scope, row.name]

        if ignore_duplicate and row.archive_contents_scope is not None:
            continue

        if (row.scope, row.name) in archive_contents_to_add:
            # Ignore duplicate input
            continue

        if row.did_scope is None:
            new_did = {}
            new_did.update((k, v) for k, v in file.items() if k != 'meta')
            for key in file.get('meta', {}):
                new_did[key] = file['meta'][key]
            new_did['constituent'] = True
            new_did['did_type'] = DIDType.FILE
            new_did['account'] = account
            dids_to_add[row.scope, row.name] = new_did

            new_content = {
                'child_scope': file['scope'],
                'child_name': file['name'],
                'scope': parent_did.scope,
                'name': parent_did.name,
                'bytes': file['bytes'],
                'adler32': file.get('adler32'),
                'md5': file.get('md5'),
                'guid': file.get('guid'),
                'length': file.get('events')
            }
        else:
            if row.did_type != DIDType.FILE:
                raise exception.UnsupportedOperation('Data identifier %s:%s of type %s cannot be added to an archive ' % (row.scope, row.name, row.did_type))

            if not row.constituent:
                must_set_constituent = True

            new_content = {
                'child_scope': row.scope,
                'child_name': row.name,
                'scope': parent_did.scope,
                'name': parent_did.name,
                'bytes': row.bytes,
                'adler32': row.adler32,
                'md5': row.md5,
                'guid': row.guid,
                'length': row.events
            }

        archive_contents_to_add[row.scope, row.name] = new_content

    # insert into archive_contents
    try:
        values = list(dids_to_add.values())
        stmt = insert(
            models.DataIdentifier
        )
        dids_to_add and session.execute(stmt, values)
        values = list(archive_contents_to_add.values())
        stmt = insert(
            models.ConstituentAssociation
        )
        archive_contents_to_add and session.execute(stmt, values)
        if must_set_constituent:
            stmt = update(
                models.DataIdentifier
            ).where(
                exists(
                    select(1)
                ).where(
                    and_(models.DataIdentifier.scope == files_temp_table.scope,
                         models.DataIdentifier.name == files_temp_table.name)
                )
            ).where(
                or_(models.DataIdentifier.constituent.is_(None),
                    models.DataIdentifier.constituent == false())
            ).values({
                models.DataIdentifier.constituent: True
            }).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)

    if not parent_did.is_archive:
        # mark the archive file as is_archive
        parent_did.is_archive = True

        # mark parent datasets as is_archive = True
        stmt = update(
            models.DataIdentifier
        ).where(
            exists(
                select(1).prefix_with("/*+ INDEX(CONTENTS CONTENTS_CHILD_SCOPE_NAME_IDX) */", dialect="oracle")
            ).where(
                and_(models.DataIdentifierAssociation.child_scope == parent_did.scope,
                     models.DataIdentifierAssociation.child_name == parent_did.name,
                     models.DataIdentifierAssociation.scope == models.DataIdentifier.scope,
                     models.DataIdentifierAssociation.name == models.DataIdentifier.name)
            )
        ).where(
            or_(models.DataIdentifier.is_archive.is_(None),
                models.DataIdentifier.is_archive == false())
        ).values({
            models.DataIdentifier.is_archive: True
        }).execution_options(
            synchronize_session=False
        )
        session.execute(stmt)


@transactional_session
def __add_files_to_dataset(
    parent_did: models.DataIdentifier,
    files_temp_table: Any,
    files: "Mapping[tuple[InternalScope, str], Mapping[str, Any]]",
    account: "InternalAccount",
    rse_id: str,
    ignore_duplicate: bool = False,
    *,
    session: "Session"
) -> dict[tuple["InternalScope", str], dict[str, Any]]:
    """
    Add files to dataset.

    :param parent_did:         the DataIdentifier object of the parent did
    :param files_temp_table:   Temporary table containing the scope and name of files to add.
    :param account:            The account owner.
    :param rse_id:             The RSE id for the replicas.
    :param ignore_duplicate:   If True, ignore duplicate entries.
    :param session:            The database session in use.
    :returns:                  List of files attached (excluding the ones that were already attached to the dataset).
    """
    # Get metadata from dataset
    try:
        dataset_meta = validate_name(scope=parent_did.scope, name=parent_did.name, did_type='D')
    except Exception:
        dataset_meta = None

    if rse_id:
        # Tier-0 uses this old work-around to register replicas on the RSE
        # in the same call as attaching them to a dataset
        rucio.core.replica.add_replicas(rse_id=rse_id, files=files.values(), dataset_meta=dataset_meta,
                                        account=account, session=session)

    stmt = select(
        files_temp_table.scope,
        files_temp_table.name,
        models.DataIdentifier.scope.label('did_scope'),
        models.DataIdentifier.bytes,
        models.DataIdentifier.guid,
        models.DataIdentifier.events,
        models.DataIdentifier.availability,
        models.DataIdentifier.adler32,
        models.DataIdentifier.md5,
        models.DataIdentifier.is_archive,
        models.DataIdentifier.did_type,
    ).outerjoin_from(
        files_temp_table,
        models.DataIdentifier,
        and_(models.DataIdentifier.scope == files_temp_table.scope,
             models.DataIdentifier.name == files_temp_table.name),
    )
    if ignore_duplicate:
        stmt = stmt.add_columns(
            models.DataIdentifierAssociation.scope.label('contents_scope'),
        ).outerjoin_from(
            files_temp_table,
            models.DataIdentifierAssociation,
            and_(models.DataIdentifierAssociation.scope == parent_did.scope,
                 models.DataIdentifierAssociation.name == parent_did.name,
                 models.DataIdentifierAssociation.child_scope == files_temp_table.scope,
                 models.DataIdentifierAssociation.child_name == files_temp_table.name),
        )

    files_to_add = {}
    for row in session.execute(stmt):
        file = files[row.scope, row.name]

        if row.did_scope is None:
            raise exception.DataIdentifierNotFound(f"Data identifier '{row.scope}:{row.name}' not found")

        if row.availability == DIDAvailability.LOST:
            raise exception.UnsupportedOperation('File %s:%s is LOST and cannot be attached' % (row.scope, row.name))

        if row.did_type != DIDType.FILE:
            raise exception.UnsupportedOperation('Data identifier %s:%s of type %s cannot be added to a dataset ' % (row.scope, row.name, row.did_type))

        # Check meta-data, if provided
        row_dict = row._asdict()
        for key in ['bytes', 'adler32', 'md5']:
            if key in file and str(file[key]) != str(row_dict[key]):
                raise exception.FileConsistencyMismatch(key + " mismatch for '%(scope)s:%(name)s': " % row_dict + str(file.get(key)) + '!=' + str(row_dict[key]))

        if ignore_duplicate and row.contents_scope is not None:
            continue

        if (row.scope, row.name) in files_to_add:
            # Ignore duplicate input files
            continue

        if row.is_archive and not parent_did.is_archive:
            parent_did.is_archive = True

        files_to_add[(row.scope, row.name)] = {
            'scope': parent_did.scope,
            'name': parent_did.name,
            'child_scope': row.scope,
            'child_name': row.name,
            'bytes': row.bytes,
            'adler32': row.adler32,
            'md5': row.md5,
            'guid': row.guid,
            'events': row.events,
            'did_type': DIDType.DATASET,
            'child_type': DIDType.FILE,
            'rule_evaluation': True,
        }

    try:
        values = list(files_to_add.values())
        stmt = insert(
            models.DataIdentifierAssociation
        )
        files_to_add and session.execute(stmt, values)
        session.flush()
        return files_to_add
    except IntegrityError as error:
        if match('.*IntegrityError.*ORA-02291: integrity constraint .*CONTENTS_CHILD_ID_FK.*violated - parent key not found.*', error.args[0]) \
                or match('.*IntegrityError.*1452.*Cannot add or update a child row: a foreign key constraint fails.*', error.args[0]) \
                or match('.*IntegrityError.*foreign key constraints? failed.*', error.args[0]) \
                or match('.*IntegrityError.*insert or update on table.*violates foreign key constraint.*', error.args[0]):
            raise exception.DataIdentifierNotFound("Data identifier not found")
        elif match('.*IntegrityError.*ORA-00001: unique constraint .*CONTENTS_PK.*violated.*', error.args[0]) \
                or match('.*IntegrityError.*UNIQUE constraint failed: contents.scope, contents.name, contents.child_scope, contents.child_name.*', error.args[0]) \
                or match('.*IntegrityError.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*UniqueViolation.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*IntegrityError.*1062.*Duplicate entry .*for key.*PRIMARY.*', error.args[0]) \
                or match('.*duplicate entry.*key.*PRIMARY.*', error.args[0]) \
                or match('.*IntegrityError.*columns? .*not unique.*', error.args[0]):
            raise exception.FileAlreadyExists(error.args)
        else:
            raise exception.RucioException(error.args)


@transactional_session
def __add_collections_to_container(
    parent_did: models.DataIdentifier,
    collections_temp_table: Any,
    collections: "Mapping[tuple[InternalScope, str], Mapping[str, Any]]",
    account: "InternalAccount",
    ignore_duplicate: bool = False,
    *,
    session: "Session"
) -> None:
    """
    Add collections (datasets or containers) to container.

    :param parent_did: the DataIdentifier object of the parent did
    :param collections: .
    :param account: The account owner.
    :param ignore_duplicate: If True, ignore duplicate entries.
    :param session: The database session in use.
    """

    if (parent_did.scope, parent_did.name) in collections:
        raise exception.UnsupportedOperation('Self-append is not valid!')

    stmt = select(
        collections_temp_table.scope,
        collections_temp_table.name,
        models.DataIdentifier.scope.label('did_scope'),
        models.DataIdentifier.did_type
    ).outerjoin_from(
        collections_temp_table,
        models.DataIdentifier,
        and_(models.DataIdentifier.scope == collections_temp_table.scope,
             models.DataIdentifier.name == collections_temp_table.name),
    )
    if ignore_duplicate:
        stmt = stmt.add_columns(
            models.DataIdentifierAssociation.scope.label("ignore_duplicate_parent_scope"),
            models.DataIdentifierAssociation.name.label("ignore_duplicate_parent_name"),
            collections_temp_table.scope.label("ignore_duplicate_child_scope"),
            collections_temp_table.name.label("ignore_duplicate_child_name"),
        ).outerjoin_from(
            collections_temp_table,
            models.DataIdentifierAssociation,
            and_(
                models.DataIdentifierAssociation.scope == parent_did.scope,
                models.DataIdentifierAssociation.name == parent_did.name,
                models.DataIdentifierAssociation.child_scope == collections_temp_table.scope,
                models.DataIdentifierAssociation.child_name == collections_temp_table.name,
            ),
        )

    container_parents = None
    child_type = None
    ignore_duplicate_existing_attachments = set()

    for row in session.execute(stmt):

        if row.did_scope is None:
            raise exception.DataIdentifierNotFound("Data identifier '%(scope)s:%(name)s' not found" % row)

        if row.did_type == DIDType.FILE:
            raise exception.UnsupportedOperation("Adding a file (%s:%s) to a container (%s:%s) is forbidden" % (row.scope, row.name, parent_did.scope, parent_did.name))

        if not child_type:
            child_type = row.did_type

        if child_type != row.did_type:
            raise exception.UnsupportedOperation("Mixed collection is not allowed: '%s:%s' is a %s(expected type: %s)" % (row.scope, row.name, row.did_type, child_type))

        if child_type == DIDType.CONTAINER:
            if container_parents is None:
                container_parents = {(parent['scope'], parent['name']) for parent in list_all_parent_dids(scope=parent_did.scope, name=parent_did.name, session=session)}

            if (row.scope, row.name) in container_parents:
                raise exception.UnsupportedOperation('Circular attachment detected. %s:%s is already a parent of %s:%s' % (row.scope, row.name, parent_did.scope, parent_did.name))

        if ignore_duplicate and row.ignore_duplicate_parent_scope is not None:
            ignore_duplicate_existing_attachments.add((row.ignore_duplicate_parent_scope, row.ignore_duplicate_parent_name, row.ignore_duplicate_child_scope, row.ignore_duplicate_child_name))

    messages = []
    for c in collections.values():
        if ignore_duplicate and (parent_did.scope, parent_did.name, c['scope'], c['name']) in ignore_duplicate_existing_attachments:
            continue

        did_asso = models.DataIdentifierAssociation(
            scope=parent_did.scope,
            name=parent_did.name,
            child_scope=c['scope'],
            child_name=c['name'],
            did_type=DIDType.CONTAINER,
            child_type=child_type,
            rule_evaluation=True
        )
        did_asso.save(session=session, flush=False)
        # Send AMI messages
        if child_type == DIDType.CONTAINER:
            chld_type = 'CONTAINER'
        elif child_type == DIDType.DATASET:
            chld_type = 'DATASET'
        else:
            chld_type = 'UNKNOWN'

        message = {'account': account.external,
                   'scope': parent_did.scope.external,
                   'name': parent_did.name,
                   'childscope': c['scope'].external,
                   'childname': c['name'],
                   'childtype': chld_type}
        if account.vo != DEFAULT_VO:
            message['vo'] = account.vo
        messages.append(message)

    try:
        for message in messages:
            add_message('REGISTER_CNT', message, session=session)
        session.flush()
    except IntegrityError as error:
        if match('.*IntegrityError.*ORA-02291: integrity constraint .*CONTENTS_CHILD_ID_FK.*violated - parent key not found.*', error.args[0]) \
                or match('.*IntegrityError.*1452.*Cannot add or update a child row: a foreign key constraint fails.*', error.args[0]) \
                or match('.*IntegrityError.*foreign key constraints? failed.*', error.args[0]) \
                or match('.*IntegrityError.*insert or update on table.*violates foreign key constraint.*', error.args[0]):
            raise exception.DataIdentifierNotFound("Data identifier not found")
        elif match('.*IntegrityError.*ORA-00001: unique constraint .*CONTENTS_PK.*violated.*', error.args[0]) \
                or match('.*IntegrityError.*1062.*Duplicate entry .*for key.*PRIMARY.*', error.args[0]) \
                or match('.*IntegrityError.*columns? scope.*name.*child_scope.*child_name.*not unique.*', error.args[0]) \
                or match('.*IntegrityError.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*UniqueViolation.*duplicate key value violates unique constraint.*', error.args[0]) \
                or match('.*IntegrityError.* UNIQUE constraint failed: contents.scope, contents.name, contents.child_scope, contents.child_name.*', error.args[0]):
            raise exception.DuplicateContent(error.args)
        raise exception.RucioException(error.args)


@transactional_session
def delete_dids(
        dids: "Sequence[Mapping[str, Any]]",
        account: "InternalAccount",
        expire_rules: bool = False,
        *,
        session: "Session",
        logger: "LoggerFunction" = logging.log,
) -> None:
    """
    Delete data identifiers

    :param dids:          The list of DIDs to delete.
    :param account:       The account.
    :param expire_rules:  Expire large rules instead of deleting them right away. This should only be used in Undertaker mode, as it can be that
                          the method returns normally, but a DID was not deleted; This trusts in the fact that the undertaker will retry an
                          expired DID.
    :param session:       The database session in use.
    :param logger:        Optional decorated logger that can be passed from the calling daemons or servers.
    """
    if not dids:
        return

    not_purge_replicas = []

    archive_dids = config_get_bool('deletion', 'archive_dids', default=False, session=session)
    archive_content = config_get_bool('deletion', 'archive_content', default=False, session=session)

    file_dids = {}
    collection_dids = {}
    all_dids = {}
    for did in dids:
        scope, name = did['scope'], did['name']
        logger(logging.INFO, 'Removing did %(scope)s:%(name)s (%(did_type)s)' % did)
        all_dids[scope, name] = {'scope': scope, 'name': name}
        if did['did_type'] == DIDType.FILE:
            file_dids[scope, name] = {'scope': scope, 'name': name}
        else:
            collection_dids[scope, name] = {'scope': scope, 'name': name}

        # ATLAS LOCALGROUPDISK Archive policy
        if did['did_type'] == DIDType.DATASET and did['scope'].external != 'archive':
            try:
                rucio.core.rule.archive_localgroupdisk_datasets(scope=did['scope'], name=did['name'], session=session)
            except exception.UndefinedPolicy:
                pass

        if did['purge_replicas'] is False:
            not_purge_replicas.append((did['scope'], did['name']))

        if archive_content:
            insert_content_history(filter_=[and_(models.DataIdentifierAssociation.scope == did['scope'],
                                                 models.DataIdentifierAssociation.name == did['name'])],
                                   did_created_at=did.get('created_at'),
                                   session=session)

        # Send message
        message = {'account': account.external,
                   'scope': did['scope'].external,
                   'name': did['name']}
        if did['scope'].vo != DEFAULT_VO:
            message['vo'] = did['scope'].vo

        add_message('ERASE', message, session=session)

    if not file_dids:
        data_in_temp_table = all_dids = collection_dids
    elif not collection_dids:
        data_in_temp_table = all_dids = file_dids
    else:
        data_in_temp_table = all_dids

    if not all_dids:
        return

    temp_table = temp_table_mngr(session).create_scope_name_table()
    values = list(data_in_temp_table.values())
    stmt = insert(
        temp_table
    )
    session.execute(stmt, values)

    # Delete rules on DID
    skip_deletion = False  # Skip deletion in case of expiration of a rule
    with METRICS.timer('delete_dids.rules'):
        stmt = select(
            models.ReplicationRule.id,
            models.ReplicationRule.scope,
            models.ReplicationRule.name,
            models.ReplicationRule.rse_expression,
            models.ReplicationRule.locks_ok_cnt,
            models.ReplicationRule.locks_replicating_cnt,
            models.ReplicationRule.locks_stuck_cnt
        ).join_from(
            temp_table,
            models.ReplicationRule,
            and_(models.ReplicationRule.scope == temp_table.scope,
                 models.ReplicationRule.name == temp_table.name)
        )
        for (rule_id, scope, name, rse_expression, locks_ok_cnt, locks_replicating_cnt, locks_stuck_cnt) in session.execute(stmt):
            logger(logging.DEBUG, 'Removing rule %s for did %s:%s on RSE-Expression %s' % (str(rule_id), scope, name, rse_expression))

            # Propagate purge_replicas from DID to rules
            if (scope, name) in not_purge_replicas:
                purge_replicas = False
            else:
                purge_replicas = True
            if expire_rules and locks_ok_cnt + locks_replicating_cnt + locks_stuck_cnt > int(config_get_int('undertaker', 'expire_rules_locks_size', default=10000, session=session)):
                # Expire the rule (soft=True)
                rucio.core.rule.delete_rule(rule_id=rule_id, purge_replicas=purge_replicas, soft=True, delete_parent=True, nowait=True, session=session)
                # Update expiration of DID
                set_metadata(scope=scope, name=name, key='lifetime', value=3600 * 24, session=session)
                skip_deletion = True
            else:
                rucio.core.rule.delete_rule(rule_id=rule_id, purge_replicas=purge_replicas, delete_parent=True, nowait=True, session=session)

    if skip_deletion:
        return

    # Detach from parent DIDs:
    existing_parent_dids = False
    with METRICS.timer('delete_dids.parent_content'):
        stmt = select(
            models.DataIdentifierAssociation
        ).join_from(
            temp_table,
            models.DataIdentifierAssociation,
            and_(models.DataIdentifierAssociation.child_scope == temp_table.scope,
                 models.DataIdentifierAssociation.child_name == temp_table.name)
        )
        for parent_did in session.execute(stmt).scalars():
            existing_parent_dids = True
            detach_dids(scope=parent_did.scope, name=parent_did.name, dids=[{'scope': parent_did.child_scope, 'name': parent_did.child_name}], session=session)

    # Remove generic DID metadata
    must_delete_did_meta = True
    if session.bind.dialect.name == 'oracle':
        oracle_version = int(session.connection().connection.version.split('.')[0])
        if oracle_version < 12:
            must_delete_did_meta = False
    if must_delete_did_meta:
        stmt = delete(
            models.DidMeta
        ).where(
            exists(
                select(1)
            ).where(
                and_(models.DidMeta.scope == temp_table.scope,
                     models.DidMeta.name == temp_table.name)
            )
        ).execution_options(
            synchronize_session=False
        )
        with METRICS.timer('delete_dids.did_meta'):
            session.execute(stmt)

    # Prepare the common part of the query for updating bad replicas if they exist
    bad_replica_stmt = update(
        models.BadReplica
    ).where(
        models.BadReplica.state == BadFilesStatus.BAD
    ).values({
        models.BadReplica.state: BadFilesStatus.DELETED,
        models.BadReplica.updated_at: datetime.utcnow(),
    }).execution_options(
        synchronize_session=False
    )

    if file_dids:
        if data_in_temp_table is not file_dids:
            stmt = delete(
                temp_table
            )
            session.execute(stmt)

            values = list(file_dids.values())
            stmt = insert(
                temp_table
            )
            session.execute(stmt, values)
            data_in_temp_table = file_dids

        # update bad files passed directly as input
        stmt = bad_replica_stmt.where(
            exists(
                select(1)
            ).where(
                and_(models.BadReplica.scope == temp_table.scope,
                     models.BadReplica.name == temp_table.name)
            )
        )
        session.execute(stmt)

    if collection_dids:
        if data_in_temp_table is not collection_dids:
            stmt = delete(
                temp_table
            )
            session.execute(stmt)

            values = list(collection_dids.values())
            stmt = insert(
                temp_table
            )
            session.execute(stmt, values)
            data_in_temp_table = collection_dids

        # Find files of datasets passed as input and put them in a separate temp table
        resolved_files_temp_table = temp_table_mngr(session).create_scope_name_table()
        stmt = insert(
            resolved_files_temp_table,
        ).from_select(
            ['scope', 'name'],
            select(
                models.DataIdentifierAssociation.child_scope,
                models.DataIdentifierAssociation.child_name,
            ).distinct(
            ).join_from(
                temp_table,
                models.DataIdentifierAssociation,
                and_(models.DataIdentifierAssociation.scope == temp_table.scope,
                     models.DataIdentifierAssociation.name == temp_table.name)
            ).where(
                models.DataIdentifierAssociation.child_type == DIDType.FILE
            )
        )
        session.execute(stmt)

        # update bad files from datasets
        stmt = bad_replica_stmt.where(
            exists(
                select(1)
            ).where(
                and_(models.BadReplica.scope == resolved_files_temp_table.scope,
                     models.BadReplica.name == resolved_files_temp_table.name)
            )
        )
        session.execute(stmt)

        # Set Epoch tombstone for the files replicas inside the DID
        if config_get_bool('undertaker', 'purge_all_replicas', default=False, session=session):
            with METRICS.timer('delete_dids.file_content'):
                stmt = update(
                    models.RSEFileAssociation
                ).where(
                    exists(
                        select(1)
                    ).where(
                        and_(models.RSEFileAssociation.scope == resolved_files_temp_table.scope,
                             models.RSEFileAssociation.name == resolved_files_temp_table.name)
                    )
                ).where(
                    and_(models.RSEFileAssociation.lock_cnt == 0,
                         models.RSEFileAssociation.tombstone != null())
                ).values({
                    models.RSEFileAssociation.tombstone: datetime(1970, 1, 1)
                }).execution_options(
                    synchronize_session=False
                )
                session.execute(stmt)

        # Remove content
        with METRICS.timer('delete_dids.content'):
            stmt = delete(
                models.DataIdentifierAssociation
            ).where(
                exists(
                    select(1)
                ).where(
                    and_(models.DataIdentifierAssociation.scope == temp_table.scope,
                         models.DataIdentifierAssociation.name == temp_table.name)
                )
            ).execution_options(
                synchronize_session=False
            )
            rowcount = session.execute(stmt).rowcount
            METRICS.counter(name='delete_dids.content_rowcount').inc(rowcount)

        # Remove CollectionReplica
        with METRICS.timer('delete_dids.collection_replicas'):
            stmt = delete(
                models.CollectionReplica
            ).where(
                exists(
                    select(1)
                ).where(
                    and_(models.CollectionReplica.scope == temp_table.scope,
                         models.CollectionReplica.name == temp_table.name)
                )
            ).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)

    # remove data identifier
    if existing_parent_dids:
        # Exit method early to give Judge time to remove locks (Otherwise, due to foreign keys, DID removal does not work
        logger(logging.DEBUG, 'Leaving delete_dids early for Judge-Evaluator checks')
        return

    if collection_dids:
        if data_in_temp_table is not collection_dids:
            stmt = delete(
                temp_table
            )
            session.execute(stmt)

            values = list(collection_dids.values())
            stmt = insert(
                temp_table
            )
            session.execute(stmt, values)
            data_in_temp_table = collection_dids

        with METRICS.timer('delete_dids.dids_followed'):
            stmt = delete(
                models.DidFollowed
            ).where(
                exists(
                    select(1)
                ).where(
                    and_(models.DidFollowed.scope == temp_table.scope,
                         models.DidFollowed.name == temp_table.name)
                )
            ).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)

        with METRICS.timer('delete_dids.dids'):
            dids_to_delete_filter = exists(
                select(1)
            ).where(
                and_(models.DataIdentifier.scope == temp_table.scope,
                     models.DataIdentifier.name == temp_table.name,
                     models.DataIdentifier.did_type.in_([DIDType.CONTAINER, DIDType.DATASET]))
            )

            if archive_dids:
                insert_deleted_dids(filter_=dids_to_delete_filter, session=session)

            stmt = delete(
                models.DataIdentifier
            ).where(
                dids_to_delete_filter,
            ).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)

    if file_dids:
        if data_in_temp_table is not file_dids:
            stmt = delete(
                temp_table
            )
            session.execute(stmt)

            values = list(file_dids.values())
            stmt = insert(
                temp_table
            )
            session.execute(stmt, values)
            data_in_temp_table = file_dids
        stmt = update(
            models.DataIdentifier
        ).where(
            exists(
                select(1)
            ).where(
                and_(models.DataIdentifier.scope == temp_table.scope,
                     models.DataIdentifier.name == temp_table.name)
            )
        ).where(
            models.DataIdentifier.did_type == DIDType.FILE
        ).values({
            models.DataIdentifier.expired_at: None
        }).execution_options(
            synchronize_session=False
        )
        session.execute(stmt)


@transactional_session
def detach_dids(
    scope: "InternalScope",
    name: str,
    dids: "Sequence[Mapping[str, Any]]",
    *,
    session: "Session"
) -> None:
    """
    Detach data identifier

    :param scope: The scope name.
    :param name: The data identifier name.
    :param dids: The content.
    :param session: The database session in use.
    """
    # Row Lock the parent DID
    stmt = select(
        models.DataIdentifier
    ).where(
        and_(models.DataIdentifier.scope == scope,
             models.DataIdentifier.name == name,
             or_(models.DataIdentifier.did_type == DIDType.CONTAINER,
                 models.DataIdentifier.did_type == DIDType.DATASET))
    )
    try:
        did = session.execute(stmt).scalar_one()
        # Mark for rule re-evaluation
        models.UpdatedDID(
            scope=scope,
            name=name,
            rule_evaluation_action=DIDReEvaluation.DETACH
        ).save(session=session, flush=False)
    except NoResultFound:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")

    # TODO: should judge target DID's status: open, monotonic, close.
    stmt = select(
        models.DataIdentifierAssociation
    ).where(
        and_(models.DataIdentifierAssociation.scope == scope,
             models.DataIdentifierAssociation.name == name)
    ).limit(
        1
    )
    if session.execute(stmt).scalar() is None:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' has no child data identifiers.")
    for source in dids:
        if (scope == source['scope']) and (name == source['name']):
            raise exception.UnsupportedOperation('Self-detach is not valid.')
        child_scope = source['scope']
        child_name = source['name']
        curr_stmt = stmt.where(
            and_(models.DataIdentifierAssociation.child_scope == child_scope,
                 models.DataIdentifierAssociation.child_name == child_name)
        ).limit(
            1
        )
        associ_did = session.execute(curr_stmt).scalar()
        if associ_did is None:
            raise exception.DataIdentifierNotFound(f"Data identifier '{child_scope}:{child_name}' not found under '{scope}:{name}'")

        child_type = associ_did.child_type
        child_size = associ_did.bytes
        child_events = associ_did.events
        if did.length:
            did.length -= 1
        if did.bytes and child_size:
            did.bytes -= child_size
        if did.events and child_events:
            did.events -= child_events
        associ_did.delete(session=session)

        # Archive contents
        # If reattach happens, merge the latest due to primary key constraint
        new_detach = models.DataIdentifierAssociationHistory(scope=associ_did.scope,
                                                             name=associ_did.name,
                                                             child_scope=associ_did.child_scope,
                                                             child_name=associ_did.child_name,
                                                             did_type=associ_did.did_type,
                                                             child_type=associ_did.child_type,
                                                             bytes=associ_did.bytes,
                                                             adler32=associ_did.adler32,
                                                             md5=associ_did.md5,
                                                             guid=associ_did.guid,
                                                             events=associ_did.events,
                                                             rule_evaluation=associ_did.rule_evaluation,
                                                             did_created_at=did.created_at,
                                                             created_at=associ_did.created_at,
                                                             updated_at=associ_did.updated_at,
                                                             deleted_at=datetime.utcnow())
        new_detach.save(session=session, flush=False)

        # Send message for AMI. To be removed in the future when they use the DETACH messages
        if did.did_type == DIDType.CONTAINER:
            if child_type == DIDType.CONTAINER:
                chld_type = 'CONTAINER'
            elif child_type == DIDType.DATASET:
                chld_type = 'DATASET'
            else:
                chld_type = 'UNKNOWN'

            message = {'scope': scope.external,
                       'name': name,
                       'childscope': source['scope'].external,
                       'childname': source['name'],
                       'childtype': chld_type}
            if scope.vo != DEFAULT_VO:
                message['vo'] = scope.vo

            add_message('ERASE_CNT', message, session=session)

        message = {'scope': scope.external,
                   'name': name,
                   'did_type': str(did.did_type),
                   'child_scope': source['scope'].external,
                   'child_name': str(source['name']),
                   'child_type': str(child_type)}
        if scope.vo != DEFAULT_VO:
            message['vo'] = scope.vo

        add_message('DETACH', message, session=session)


@stream_session
def list_new_dids(
    did_type: Union[str, "DIDType"],
    thread: Optional[int] = None,
    total_threads: Optional[int] = None,
    chunk_size: int = 1000,
    *,
    session: "Session",
) -> "Iterator[dict[str, Any]]":
    """
    List recent identifiers.

    :param did_type : The DID type.
    :param thread: The assigned thread for this necromancer.
    :param total_threads: The total number of threads of all necromancers.
    :param chunk_size: Number of requests to return per yield.
    :param session: The database session in use.
    """

    sub_query = select(
        1
    ).prefix_with(
        "/*+ INDEX(RULES RULES_SCOPE_NAME_IDX) */", dialect='oracle'
    ).where(
        and_(models.DataIdentifier.scope == models.ReplicationRule.scope,
             models.DataIdentifier.name == models.ReplicationRule.name,
             models.ReplicationRule.state == RuleState.INJECT)
    )

    select_stmt = select(
        models.DataIdentifier
    ).with_hint(
        models.DataIdentifier,
        'INDEX(dids DIDS_IS_NEW_IDX)',
        'oracle'
    ).where(
        and_(models.DataIdentifier.is_new == true(),
             ~exists(sub_query))
    )

    if did_type:
        if isinstance(did_type, str):
            select_stmt = select_stmt.where(
                models.DataIdentifier.did_type == DIDType[did_type]
            )
        elif isinstance(did_type, DIDType):
            select_stmt = select_stmt.where(
                models.DataIdentifier.did_type == did_type
            )

    select_stmt = filter_thread_work(session=session, query=select_stmt, total_threads=total_threads, thread_id=thread, hash_variable='name')

    row_count = 0
    for chunk in session.execute(select_stmt).yield_per(10).scalars():
        row_count += 1
        if row_count <= chunk_size:
            yield {'scope': chunk.scope, 'name': chunk.name, 'did_type': chunk.did_type}  # TODO Change this to the proper filebytes [RUCIO-199]
        else:
            break


@transactional_session
def set_new_dids(
    dids: "Sequence[Mapping[str, Any]]",
    new_flag: Optional[bool],
    *,
    session: "Session"
) -> bool:
    """
    Set/reset the flag new

    :param dids: A list of DIDs
    :param new_flag: A boolean to flag new DIDs.
    :param session: The database session in use.
    """
    if session.bind.dialect.name == 'postgresql':
        new_flag = bool(new_flag)
    for did in dids:
        try:
            stmt = update(
                models.DataIdentifier
            ).where(
                and_(models.DataIdentifier.scope == did['scope'],
                     models.DataIdentifier.name == did['name'])
            ).values({
                models.DataIdentifier.is_new: new_flag
            }).execution_options(
                synchronize_session=False
            )
            rowcount = session.execute(stmt).rowcount
            if not rowcount:
                raise exception.DataIdentifierNotFound("Data identifier '%s:%s' not found" % (did['scope'], did['name']))
        except DatabaseError as error:
            raise exception.DatabaseException('%s : Cannot update %s:%s' % (error.args[0], did['scope'], did['name']))
    try:
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args[0])
    except DatabaseError as error:
        raise exception.RucioException(error.args[0])
    return True


@stream_session
def list_content(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List data identifier contents.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    """
    stmt = select(
        models.DataIdentifierAssociation
    ).with_hint(
        models.DataIdentifierAssociation,
        'INDEX(CONTENTS CONTENTS_PK)',
        'oracle'
    ).where(
        and_(models.DataIdentifierAssociation.scope == scope,
             models.DataIdentifierAssociation.name == name)
    )
    children_found = False
    for tmp_did in session.execute(stmt).yield_per(5).scalars():
        children_found = True
        yield {'scope': tmp_did.child_scope, 'name': tmp_did.child_name, 'type': tmp_did.child_type,
               'bytes': tmp_did.bytes, 'adler32': tmp_did.adler32, 'md5': tmp_did.md5}
    if not children_found:
        # Raise exception if the DID doesn't exist
        __get_did(scope=scope, name=name, session=session)


@stream_session
def list_content_history(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List data identifier contents history.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    """
    try:
        stmt = select(
            models.DataIdentifierAssociationHistory
        ).where(
            and_(models.DataIdentifierAssociationHistory.scope == scope,
                 models.DataIdentifierAssociationHistory.name == name)
        )
        for tmp_did in session.execute(stmt).yield_per(5).scalars():
            yield {'scope': tmp_did.child_scope, 'name': tmp_did.child_name,
                   'type': tmp_did.child_type,
                   'bytes': tmp_did.bytes, 'adler32': tmp_did.adler32, 'md5': tmp_did.md5,
                   'deleted_at': tmp_did.deleted_at, 'created_at': tmp_did.created_at,
                   'updated_at': tmp_did.updated_at}
    except NoResultFound:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")


@stream_session
def list_parent_dids(
    scope: "InternalScope",
    name: str,
    order_by: Optional[list[str]] = None,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List parent datasets and containers of a DID.

    :param scope:     The scope.
    :param name:      The name.
    :param order_by:  List of parameters to order the query by. Possible values: ['scope', 'name', 'did_type', 'created_at'].
    :param session:   The database session.
    :returns:         List of DIDs.
    :rtype:           Generator.
    """

    if order_by is None:
        order_by = []

    stmt = select(
        models.DataIdentifierAssociation.scope,
        models.DataIdentifierAssociation.name,
        models.DataIdentifierAssociation.did_type,
        models.DataIdentifier.created_at
    ).where(
        and_(models.DataIdentifierAssociation.child_scope == scope,
             models.DataIdentifierAssociation.child_name == name,
             models.DataIdentifier.scope == models.DataIdentifierAssociation.scope,
             models.DataIdentifier.name == models.DataIdentifierAssociation.name)
    ).order_by(
        *order_by
    )

    for did in session.execute(stmt).yield_per(5):
        yield {'scope': did.scope, 'name': did.name, 'type': did.did_type}


@stream_session
def list_all_parent_dids(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List all parent datasets and containers of a DID, no matter on what level.

    :param scope:     The scope.
    :param name:      The name.
    :param session:   The database session.
    :returns:         List of DIDs.
    :rtype:           Generator.
    """

    stmt = select(
        models.DataIdentifierAssociation.scope,
        models.DataIdentifierAssociation.name,
        models.DataIdentifierAssociation.did_type
    ).where(
        and_(models.DataIdentifierAssociation.child_scope == scope,
             models.DataIdentifierAssociation.child_name == name)
    )
    for did in session.execute(stmt).yield_per(5):
        yield {'scope': did.scope, 'name': did.name, 'type': did.did_type}
        # Note that only Python3 supports recursive yield, that's the reason to do the nested for.
        for pdid in list_all_parent_dids(scope=did.scope, name=did.name, session=session):
            yield {'scope': pdid['scope'], 'name': pdid['name'], 'type': pdid['type']}


def list_child_dids_stmt(
        input_dids_table: Any,
        did_type: DIDType,
) -> "Select[tuple[InternalScope, str]]":
    """
    Build and returns a query which recursively lists children DIDs of type `did_type`
    for the DIDs given as input in a scope/name (temporary) table.

    did_type defines the desired type of DIDs in the result. If set to DIDType.Dataset,
    will only resolve containers and return datasets. If set to DIDType.File, will
    also resolve the datasets and return files.
    """
    if did_type == DIDType.DATASET:
        dids_to_resolve = [DIDType.CONTAINER]
    else:
        dids_to_resolve = [DIDType.CONTAINER, DIDType.DATASET]

    # Uses a recursive SQL CTE (Common Table Expressions)
    initial_set = select(
        models.DataIdentifierAssociation.child_scope,
        models.DataIdentifierAssociation.child_name,
        models.DataIdentifierAssociation.child_type,
    ).join_from(
        input_dids_table,
        models.DataIdentifierAssociation,
        and_(models.DataIdentifierAssociation.scope == input_dids_table.scope,
             models.DataIdentifierAssociation.name == input_dids_table.name,
             models.DataIdentifierAssociation.did_type.in_(dids_to_resolve)),
    ).cte(
        recursive=True,
    )

    # Oracle doesn't support union() in recursive CTEs, so use UNION ALL
    # and a "distinct" filter later
    child_datasets_cte = initial_set.union_all(
        select(
            models.DataIdentifierAssociation.child_scope,
            models.DataIdentifierAssociation.child_name,
            models.DataIdentifierAssociation.child_type,
        ).where(
            and_(models.DataIdentifierAssociation.scope == initial_set.c.child_scope,
                 models.DataIdentifierAssociation.name == initial_set.c.child_name,
                 models.DataIdentifierAssociation.did_type.in_(dids_to_resolve))
        )
    )

    stmt = select(
        child_datasets_cte.c.child_scope.label('scope'),
        child_datasets_cte.c.child_name.label('name'),
    ).distinct(
    ).where(
        child_datasets_cte.c.child_type == did_type,
    )
    return stmt


def list_one_did_childs_stmt(
        scope: "InternalScope",
        name: str,
        did_type: DIDType,
) -> "Select[tuple[InternalScope, str]]":
    """
    Returns the sqlalchemy query for recursively fetching the child DIDs of type
    'did_type' for the input did.

    did_type defines the desired type of DIDs in the result. If set to DIDType.Dataset,
    will only resolve containers and return datasets. If set to DIDType.File, will
    also resolve the datasets and return files.
    """
    if did_type == DIDType.DATASET:
        dids_to_resolve = [DIDType.CONTAINER]
    else:
        dids_to_resolve = [DIDType.CONTAINER, DIDType.DATASET]

    # Uses a recursive SQL CTE (Common Table Expressions)
    initial_set = select(
        models.DataIdentifierAssociation.child_scope,
        models.DataIdentifierAssociation.child_name,
        models.DataIdentifierAssociation.child_type,
    ).where(
        and_(models.DataIdentifierAssociation.scope == scope,
             models.DataIdentifierAssociation.name == name,
             models.DataIdentifierAssociation.did_type.in_(dids_to_resolve))
    ).cte(
        recursive=True,
    )

    # Oracle doesn't support union() in recursive CTEs, so use UNION ALL
    # and a "distinct" filter later
    child_datasets_cte = initial_set.union_all(
        select(
            models.DataIdentifierAssociation.child_scope,
            models.DataIdentifierAssociation.child_name,
            models.DataIdentifierAssociation.child_type,
        ).where(
            and_(models.DataIdentifierAssociation.scope == initial_set.c.child_scope,
                 models.DataIdentifierAssociation.name == initial_set.c.child_name,
                 models.DataIdentifierAssociation.did_type.in_(dids_to_resolve))
        )
    )

    stmt = select(
        child_datasets_cte.c.child_scope.label('scope'),
        child_datasets_cte.c.child_name.label('name'),
    ).distinct(
    ).where(
        child_datasets_cte.c.child_type == did_type,
    )
    return stmt


@transactional_session
def list_child_datasets(
        scope: "InternalScope",
        name: str,
        *,
        session: "Session"
) -> list[dict[str, Union["InternalScope", str]]]:
    """
    List all child datasets of a container.

    :param scope:     The scope.
    :param name:      The name.
    :param session:   The database session
    :returns:         List of DIDs
    :rtype:           Generator
    """
    stmt = list_one_did_childs_stmt(scope, name, did_type=DIDType.DATASET)
    result = []
    for row in session.execute(stmt):
        result.append({'scope': row.scope, 'name': row.name})

    return result


@stream_session
def bulk_list_files(
    dids: "Iterable[Mapping[str, Any]]",
    long: bool = False,
    *,
    session: "Session"
) -> "Optional[Iterator[dict[str, Any]]]":
    """
    List file contents of a list of data identifier.

    :param dids:       A list of DIDs.
    :param long:       A boolean to choose if more metadata are returned or not.
    :param session:    The database session in use.
    """
    for did in dids:
        try:
            for file_dict in list_files(scope=did['scope'], name=did['name'], long=long, session=session):
                file_dict['parent_scope'] = did['scope']
                file_dict['parent_name'] = did['name']
                yield file_dict
        except exception.DataIdentifierNotFound:
            pass


@stream_session
def list_files(scope: "InternalScope", name: str, long: bool = False, *, session: "Session") -> "Iterator[dict[str, Any]]":
    """
    List data identifier file contents.

    :param scope:      The scope name.
    :param name:       The data identifier name.
    :param long:       A boolean to choose if more metadata are returned or not.
    :param session:    The database session in use.
    """
    try:
        stmt = select(
            models.DataIdentifier.scope,
            models.DataIdentifier.name,
            models.DataIdentifier.bytes,
            models.DataIdentifier.adler32,
            models.DataIdentifier.guid,
            models.DataIdentifier.events,
            models.DataIdentifier.lumiblocknr,
            models.DataIdentifier.did_type
        ).with_hint(
            models.DataIdentifier,
            'INDEX(DIDS DIDS_PK)',
            'oracle'
        ).where(
            and_(models.DataIdentifier.scope == scope,
                 models.DataIdentifier.name == name)
        )
        did = session.execute(stmt).one()

        if did[7] == DIDType.FILE:
            if long:
                yield {'scope': did[0], 'name': did[1], 'bytes': did[2],
                       'adler32': did[3], 'guid': did[4] and did[4].upper(),
                       'events': did[5], 'lumiblocknr': did[6]}
            else:
                yield {'scope': did[0], 'name': did[1], 'bytes': did[2],
                       'adler32': did[3], 'guid': did[4] and did[4].upper(),
                       'events': did[5]}
        else:
            cnt_query = select(
                models.DataIdentifierAssociation.child_scope,
                models.DataIdentifierAssociation.child_name,
                models.DataIdentifierAssociation.child_type
            ).with_hint(
                models.DataIdentifierAssociation,
                'INDEX(CONTENTS CONTENTS_PK)',
                'oracle'
            )

            if long:
                dst_cnt_query = select(
                    models.DataIdentifierAssociation.child_scope,
                    models.DataIdentifierAssociation.child_name,
                    models.DataIdentifierAssociation.child_type,
                    models.DataIdentifierAssociation.bytes,
                    models.DataIdentifierAssociation.adler32,
                    models.DataIdentifierAssociation.guid,
                    models.DataIdentifierAssociation.events,
                    models.DataIdentifier.lumiblocknr
                ).with_hint(
                    models.DataIdentifierAssociation,
                    'INDEX_RS_ASC(DIDS DIDS_PK) INDEX_RS_ASC(CONTENTS CONTENTS_PK) NO_INDEX_FFS(CONTENTS CONTENTS_PK)',
                    'oracle'
                ).where(
                    and_(models.DataIdentifier.scope == models.DataIdentifierAssociation.child_scope,
                         models.DataIdentifier.name == models.DataIdentifierAssociation.child_name)
                )
            else:
                dst_cnt_query = select(
                    models.DataIdentifierAssociation.child_scope,
                    models.DataIdentifierAssociation.child_name,
                    models.DataIdentifierAssociation.child_type,
                    models.DataIdentifierAssociation.bytes,
                    models.DataIdentifierAssociation.adler32,
                    models.DataIdentifierAssociation.guid,
                    models.DataIdentifierAssociation.events,
                    bindparam("lumiblocknr", None)
                ).with_hint(
                    models.DataIdentifierAssociation,
                    'INDEX(CONTENTS CONTENTS_PK)',
                    'oracle'
                )

            dids = [(scope, name, did[7]), ]
            while dids:
                s, n, t = dids.pop()
                if t == DIDType.DATASET:
                    stmt = dst_cnt_query.where(
                        and_(models.DataIdentifierAssociation.scope == s,
                             models.DataIdentifierAssociation.name == n)
                    )

                    for child_scope, child_name, child_type, bytes_, adler32, guid, events, lumiblocknr in session.execute(stmt).yield_per(500):
                        if long:
                            yield {'scope': child_scope, 'name': child_name,
                                   'bytes': bytes_, 'adler32': adler32,
                                   'guid': guid and guid.upper(),
                                   'events': events,
                                   'lumiblocknr': lumiblocknr}
                        else:
                            yield {'scope': child_scope, 'name': child_name,
                                   'bytes': bytes_, 'adler32': adler32,
                                   'guid': guid and guid.upper(),
                                   'events': events}
                else:
                    stmt = cnt_query.where(
                        and_(models.DataIdentifierAssociation.scope == s,
                             models.DataIdentifierAssociation.name == n)
                    )
                    for child_scope, child_name, child_type in session.execute(stmt).yield_per(500):
                        dids.append((child_scope, child_name, child_type))

    except NoResultFound:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")


@stream_session
def scope_list(
    scope: "InternalScope",
    name: Optional[str] = None,
    recursive: bool = False,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List data identifiers in a scope.

    :param scope: The scope name.
    :param session: The database session in use.
    :param name: The data identifier name.
    :param recursive: boolean, True or False.
    """
    # TODO= Perf. tuning of the method
    # query = session.query(models.DataIdentifier).filter_by(scope=scope, deleted=False)
    # for did in query.yield_per(5):
    #    yield {'scope': did.scope, 'name': did.name, 'type': did.did_type, 'parent': None, 'level': 0}

    def __topdids(scope):
        sub_stmt = select(
            models.DataIdentifierAssociation.child_name
        ).where(
            and_(models.DataIdentifierAssociation.scope == scope,
                 models.DataIdentifierAssociation.child_scope == scope)
        )
        stmt = select(
            models.DataIdentifier.name,
            models.DataIdentifier.did_type,
            models.DataIdentifier.bytes
        ).where(
            and_(models.DataIdentifier.scope == scope,
                 not_(models.DataIdentifier.name.in_(sub_stmt)))
        ).order_by(
            models.DataIdentifier.name
        )
        for row in session.execute(stmt).yield_per(5):
            if row.did_type == DIDType.FILE:
                yield {'scope': scope, 'name': row.name, 'type': row.did_type, 'parent': None, 'level': 0, 'bytes': row.bytes}
            else:
                yield {'scope': scope, 'name': row.name, 'type': row.did_type, 'parent': None, 'level': 0, 'bytes': None}

    def __diddriller(pdid: "Mapping[str, Any]") -> "Iterator[dict[str, Any]]":
        stmt = select(
            models.DataIdentifierAssociation
        ).where(
            and_(models.DataIdentifierAssociation.scope == pdid['scope'],
                 models.DataIdentifierAssociation.name == pdid['name'])
        ).order_by(
            models.DataIdentifierAssociation.child_name
        )
        for row in session.execute(stmt).yield_per(5).scalars():
            parent = {'scope': pdid['scope'], 'name': pdid['name']}
            cdid = {'scope': row.child_scope, 'name': row.child_name, 'type': row.child_type, 'parent': parent, 'level': pdid['level'] + 1}
            yield cdid
            if cdid['type'] != DIDType.FILE and recursive:
                for did in __diddriller(cdid):
                    yield did

    if name is None:
        topdids = __topdids(scope)
    else:
        stmt = select(
            models.DataIdentifier
        ).where(
            and_(models.DataIdentifier.scope == scope,
                 models.DataIdentifier.name == name)
        ).limit(
            1
        )
        topdids = session.execute(stmt).scalar()
        if topdids is None:
            raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")
        topdids = [{'scope': topdids.scope, 'name': topdids.name, 'type': topdids.did_type, 'parent': None, 'level': 0}]

    if name is None:
        for topdid in topdids:
            yield topdid
            if recursive:
                for did in __diddriller(topdid):
                    yield did
    else:
        for topdid in topdids:
            for did in __diddriller(topdid):
                yield did


@read_session
def __get_did(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> "models.DataIdentifier":
    try:
        stmt = select(
            models.DataIdentifier
        ).with_hint(
            models.DataIdentifier,
            'INDEX(DIDS DIDS_PK)',
            'oracle'
        ).where(
            and_(models.DataIdentifier.scope == scope,
                 models.DataIdentifier.name == name)
        )
        return session.execute(stmt).scalar_one()
    except NoResultFound:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")


@read_session
def get_did(scope: "InternalScope", name: str, dynamic_depth: "Optional[DIDType]" = None, *, session: "Session") -> "dict[str, Any]":
    """
    Retrieve a single data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param dynamic_depth: the DID type to use as source for estimation of this DIDs length/bytes.
    If set to None, or to a value which doesn't make sense (ex: requesting depth = CONTAINER for a DID of type DATASET)
    will not compute the size dynamically.
    :param session: The database session in use.
    """
    did = __get_did(scope=scope, name=name, session=session)

    bytes_, length = did.bytes, did.length
    if dynamic_depth:
        bytes_, length, events = __resolve_bytes_length_events_did(did=did, dynamic_depth=dynamic_depth, session=session)

    if did.did_type == DIDType.FILE:
        return {'scope': did.scope, 'name': did.name, 'type': did.did_type,
                'account': did.account, 'bytes': bytes_, 'length': 1,
                'md5': did.md5, 'adler32': did.adler32}
    else:
        return {'scope': did.scope, 'name': did.name, 'type': did.did_type,
                'account': did.account, 'open': did.is_open,
                'monotonic': did.monotonic, 'expired_at': did.expired_at,
                'length': length, 'bytes': bytes_}


@transactional_session
def set_metadata(
    scope: "InternalScope",
    name: str,
    key: str,
    value: Any,
    did_type: Optional[DIDType] = None,
    did: Optional["Mapping[str, Any]"] = None,
    recursive: bool = False,
    *,
    session: "Session"
) -> None:
    """
    Add single metadata to a data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param key: the key.
    :param value: the value.
    :param did: The data identifier info.
    :param recursive: Option to propagate the metadata change to content.
    :param session: The database session in use.
    """
    did_meta_plugins.set_metadata(scope=scope, name=name, key=key, value=value, recursive=recursive, session=session)


@transactional_session
def set_metadata_bulk(
    scope: "InternalScope",
    name: str,
    meta: "Mapping[str, Any]",
    recursive: bool = False,
    *,
    session: "Session"
) -> None:
    """
    Add metadata to a data identifier.

    :param scope: The scope name.
    :param name: The data identifier name.
    :param meta: the key-values.
    :param recursive: Option to propagate the metadata change to content.
    :param session: The database session in use.
    """
    did_meta_plugins.set_metadata_bulk(scope=scope, name=name, meta=meta, recursive=recursive, session=session)


@transactional_session
def set_dids_metadata_bulk(
    dids: "Iterable[Mapping[str, Any]]",
    recursive: bool = False,
    *,
    session: "Session"
) -> None:
    """
    Add metadata to a list of data identifiers.

    :param dids: A list of DIDs including metadata.
    :param recursive: Option to propagate the metadata change to content.
    :param session: The database session in use.
    """

    for did in dids:
        did_meta_plugins.set_metadata_bulk(scope=did['scope'], name=did['name'], meta=did['meta'], recursive=recursive, session=session)


@read_session
def get_metadata(
    scope: "InternalScope",
    name: str,
    plugin: str = 'DID_COLUMN',
    *,
    session: "Session"
) -> dict[str, Any]:
    """
    Get data identifier metadata

    :param scope: The scope name.
    :param name: The data identifier name.
    :param plugin: The metadata plugin to use or 'ALL' for all.
    :param session: The database session in use.


    :returns: List of HARDCODED metadata for DID.
    """
    return did_meta_plugins.get_metadata(scope, name, plugin=plugin, session=session)


@stream_session
def list_parent_dids_bulk(
    dids: "Iterable[Mapping[str, Any]]",
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List parent datasets and containers of a DID.

    :param dids:               A list of DIDs.
    :param session:            The database session in use.
    :returns:                  List of DIDs.
    :rtype:                    Generator.
    """
    condition = []
    for did in dids:
        condition.append(and_(models.DataIdentifierAssociation.child_scope == did['scope'],
                              models.DataIdentifierAssociation.child_name == did['name']))

    try:
        for chunk in chunks(condition, 50):
            stmt = select(
                models.DataIdentifierAssociation.child_scope,
                models.DataIdentifierAssociation.child_name,
                models.DataIdentifierAssociation.scope,
                models.DataIdentifierAssociation.name,
                models.DataIdentifierAssociation.did_type
            ).where(
                or_(*chunk)
            )
            for did_chunk in session.execute(stmt).yield_per(5):
                yield {'scope': did_chunk.scope, 'name': did_chunk.name, 'child_scope': did_chunk.child_scope, 'child_name': did_chunk.child_name, 'type': did_chunk.did_type}
    except NoResultFound:
        raise exception.DataIdentifierNotFound('No Data Identifiers found')


@stream_session
def get_metadata_bulk(
    dids: list["Mapping[Any, Any]"],
    inherit: bool = False,
    plugin: str = 'JSON',
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    Get metadata for a list of DIDs
    :param dids:               A list of DIDs.
    :param inherit:            A boolean. If set to true, the metadata of the parent are concatenated.
    :param plugin:             A string. The metadata plugin to use or 'ALL' for all.
    :param session:            The database session in use.
    """
    if inherit:
        parent_list = []
        unique_dids = []
        parents = [1, ]
        depth = 0
        for did in dids:
            unique_dids.append((did['scope'], did['name']))
            parent_list.append([(did['scope'], did['name']), ])

        while parents and depth < 20:
            parents = []
            for did in list_parent_dids_bulk(dids, session=session):
                scope = did['scope']
                name = did['name']
                child_scope = did['child_scope']
                child_name = did['child_name']
                if (scope, name) not in unique_dids:
                    unique_dids.append((scope, name))
                if (scope, name) not in parents:
                    parents.append((scope, name))
                for entry in parent_list:
                    if entry[-1] == (child_scope, child_name):
                        entry.append((scope, name))
            dids = [{'scope': did[0], 'name': did[1]} for did in parents]
            depth += 1
        unique_dids = [{'scope': did[0], 'name': did[1]} for did in unique_dids]
        meta_dict = {}
        for did in unique_dids:
            try:
                meta = get_metadata(did['scope'], did['name'], plugin=plugin, session=session)
            except exception.DataIdentifierNotFound:
                meta = {}
            meta_dict[(did['scope'], did['name'])] = meta
        for dids in parent_list:
            result = {'scope': dids[0][0], 'name': dids[0][1]}
            for did in dids:
                for key in meta_dict[did]:
                    if key not in result:
                        result[key] = meta_dict[did][key]
            yield result
    else:
        condition = []
        for did in dids:
            condition.append(and_(models.DataIdentifier.scope == did['scope'],
                                  models.DataIdentifier.name == did['name']))
        try:
            for chunk in chunks(condition, 50):
                stmt = select(
                    models.DataIdentifier
                ).with_hint(
                    models.DataIdentifier,
                    'INDEX(DIDS DIDS_PK)',
                    'oracle'
                ).where(
                    or_(*chunk)
                )
                for row in session.execute(stmt).scalars():
                    yield row.to_dict()
        except NoResultFound:
            raise exception.DataIdentifierNotFound('No Data Identifiers found')


@transactional_session
def delete_metadata(
    scope: "InternalScope",
    name: str,
    key: str,
    *,
    session: "Session"
) -> None:
    """
    Delete a key from the metadata column

    :param scope: the scope of DID
    :param name: the name of the DID
    :param key: the key to be deleted
    """
    did_meta_plugins.delete_metadata(scope, name, key, session=session)


@transactional_session
def set_status(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session",
    **kwargs
) -> None:
    """
    Set data identifier status

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    :param kwargs:  Keyword arguments of the form status_name=value.
    """
    statuses = ['open', ]
    reevaluate_dids_at_close = config_get_bool('subscriptions', 'reevaluate_dids_at_close', raise_exception=False, default=False, session=session)

    update_stmt = update(
        models.DataIdentifier
    ).where(
        and_(models.DataIdentifier.scope == scope,
             models.DataIdentifier.name == name,
             or_(models.DataIdentifier.did_type == DIDType.CONTAINER,
                 models.DataIdentifier.did_type == DIDType.DATASET))
    ).prefix_with(
        "/*+ INDEX(DIDS DIDS_PK) */", dialect='oracle'
    ).execution_options(
        synchronize_session=False
    )
    values = {}
    for k in kwargs:
        if k not in statuses:
            raise exception.UnsupportedStatus(f'The status {k} is not a valid data identifier status.')
        if k == 'open':
            if not kwargs[k]:
                update_stmt = update_stmt.where(
                    and_(models.DataIdentifier.is_open == true(),
                         models.DataIdentifier.did_type != DIDType.FILE)
                )
                values['is_open'], values['closed_at'] = False, datetime.utcnow()
                values['bytes'], values['length'], values['events'] = __resolve_bytes_length_events_did(did=__get_did(scope=scope, name=name, session=session),
                                                                                                        session=session)
                # Update datasetlocks as well
                stmt = update(
                    models.DatasetLock
                ).where(
                    and_(models.DatasetLock.scope == scope,
                         models.DatasetLock.name == name)
                ).values({
                    models.DatasetLock.length: values['length'],
                    models.DatasetLock.bytes: values['bytes']
                })
                session.execute(stmt)

                # Generate a message
                message = {'scope': scope.external,
                           'name': name,
                           'bytes': values['bytes'],
                           'length': values['length'],
                           'events': values['events']}
                if scope.vo != DEFAULT_VO:
                    message['vo'] = scope.vo

                add_message('CLOSE', message, session=session)
                if reevaluate_dids_at_close:
                    set_new_dids(dids=[{'scope': scope, 'name': name}],
                                 new_flag=True,
                                 session=session)

            else:
                # Set status to open only for privileged accounts
                update_stmt = update_stmt.where(
                    and_(models.DataIdentifier.is_open == false(),
                         models.DataIdentifier.did_type != DIDType.FILE)
                )
                values['is_open'] = True

                message = {'scope': scope.external, 'name': name}
                if scope.vo != DEFAULT_VO:
                    message['vo'] = scope.vo
                add_message('OPEN', message, session=session)

    update_stmt = update_stmt.values(
        values
    )
    rowcount = session.execute(update_stmt).rowcount

    if not rowcount:
        stmt = select(
            models.DataIdentifier
        ).where(
            and_(models.DataIdentifier.scope == scope,
                 models.DataIdentifier.name == name)
        )
        try:
            session.execute(stmt).scalar_one()
        except NoResultFound:
            raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")
        raise exception.UnsupportedOperation(f"The status of the data identifier '{scope}:{name}' cannot be changed")
    else:
        # Generate callbacks
        if not values['is_open']:
            stmt = select(
                models.ReplicationRule
            ).where(
                and_(models.ReplicationRule.scope == scope,
                     models.ReplicationRule.name == name)
            )
            for rule in session.execute(stmt).scalars():
                rucio.core.rule.generate_rule_notifications(rule=rule, session=session)


@read_session
def list_dids(
    scope: "InternalScope",
    filters: "Iterable[dict[Any, Any]]",
    did_type: Literal['all', 'collection', 'dataset', 'container', 'file'] = 'collection',
    ignore_case: bool = False,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    long: bool = False,
    recursive: bool = False,
    ignore_dids: Optional["Sequence[str]"] = None,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    Search data identifiers.

    :param scope: the scope name.
    :param filters: dictionary of attributes by which the results should be filtered.
    :param did_type: the type of the DID: all(container, dataset, file), collection(dataset or container), dataset, container, file.
    :param ignore_case: ignore case distinctions.
    :param limit: limit number.
    :param offset: offset number.
    :param long: Long format option to display more information for each DID.
    :param recursive: Recursively list DIDs content.
    :param ignore_dids: List of DIDs to refrain from yielding.
    :param session: The database session in use.
    """
    return did_meta_plugins.list_dids(scope, filters, did_type, ignore_case, limit, offset, long, recursive, ignore_dids, session=session)


@read_session
def get_did_atime(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> datetime:
    """
    Get the accessed_at timestamp for a DID. Just for testing.
    :param scope: the scope name.
    :param name: The data identifier name.
    :param session: Database session to use.

    :returns: A datetime timestamp with the last access time.
    """
    stmt = select(
        models.DataIdentifier.accessed_at
    ).where(
        and_(models.DataIdentifier.scope == scope,
             models.DataIdentifier.name == name)
    )
    return session.execute(stmt).one()[0]


@read_session
def get_did_access_cnt(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> int:
    """
    Get the access_cnt for a DID. Just for testing.
    :param scope: the scope name.
    :param name: The data identifier name.
    :param session: Database session to use.

    :returns: A datetime timestamp with the last access time.
    """
    stmt = select(
        models.DataIdentifier.access_cnt
    ).where(
        and_(models.DataIdentifier.scope == scope,
             models.DataIdentifier.name == name)
    )
    return session.execute(stmt).one()[0]


@stream_session
def get_dataset_by_guid(
    guid: str,
    *,
    session: "Session"
) -> "Iterator[dict[str, Union[InternalScope, str]]]":
    """
    Get the parent datasets for a given GUID.
    :param guid: The GUID.
    :param session: Database session to use.

    :returns: A DID.
    """
    stmt = select(
        models.DataIdentifier
    ).with_hint(
        models.ReplicaLock,
        'INDEX(DIDS_GUIDS_IDX)',
        'oracle'
    ).where(
        and_(models.DataIdentifier.guid == guid,
             models.DataIdentifier.did_type == DIDType.FILE)
    )
    try:
        r = session.execute(stmt).scalar_one()
        datasets_stmt = select(
            models.DataIdentifierAssociation.scope,
            models.DataIdentifierAssociation.name
        ).with_hint(
            models.DataIdentifierAssociation,
            'INDEX(CONTENTS CONTENTS_CHILD_SCOPE_NAME_IDX)',
            'oracle'
        ).where(
            and_(models.DataIdentifierAssociation.child_scope == r.scope,
                 models.DataIdentifierAssociation.child_name == r.name)
        )

    except NoResultFound:
        raise exception.DataIdentifierNotFound("No file associated to GUID : %s" % guid)
    for tmp_did in session.execute(datasets_stmt).yield_per(5):
        yield {'scope': tmp_did.scope, 'name': tmp_did.name}


@transactional_session
def touch_dids(
    dids: "Iterable[Mapping[str, Any]]",
    *,
    session: "Session"
) -> bool:
    """
    Update the accessed_at timestamp and the access_cnt of the given DIDs.

    :param replicas: the list of DIDs.
    :param session: The database session in use.

    :returns: True, if successful, False otherwise.
    """

    now = datetime.utcnow()
    none_value = None
    try:
        for did in dids:
            stmt = update(
                models.DataIdentifier
            ).where(
                and_(models.DataIdentifier.scope == did['scope'],
                     models.DataIdentifier.name == did['name'],
                     models.DataIdentifier.did_type == did['type'])
            ).values({
                models.DataIdentifier.accessed_at: did.get('accessed_at') or now,
                models.DataIdentifier.access_cnt: case((models.DataIdentifier.access_cnt == none_value, 1),
                                                       else_=(models.DataIdentifier.access_cnt + 1))  # type: ignore
            }).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)
    except DatabaseError:
        return False

    return True


@transactional_session
def create_did_sample(
    input_scope: "InternalScope",
    input_name: str,
    output_scope: "InternalScope",
    output_name: str,
    account: "InternalAccount",
    nbfiles: str,
    *,
    session: "Session"
) -> None:
    """
    Create a sample from an input collection.

    :param input_scope: The scope of the input DID.
    :param input_name: The name of the input DID.
    :param output_scope: The scope of the output dataset.
    :param output_name: The name of the output dataset.
    :param account: The account.
    :param nbfiles: The number of files to register in the output dataset.
    :param session: The database session in use.
    """
    files = [did for did in list_files(scope=input_scope, name=input_name, long=False, session=session)]
    random.shuffle(files)
    output_files = files[:int(nbfiles)]
    add_did(scope=output_scope, name=output_name, did_type=DIDType.DATASET, account=account, statuses={}, meta=[], rules=[], lifetime=None, dids=output_files, rse_id=None, session=session)


@transactional_session
def __resolve_bytes_length_events_did(
        did: models.DataIdentifier,
        dynamic_depth: "DIDType" = DIDType.FILE,
        *, session: "Session",
) -> tuple[int, int, int]:
    """
    Resolve bytes, length and events of a DID

    :did: the DID ORM object for which we perform the resolution
    :param dynamic_depth: the DID type to use as source for estimation of this DIDs length/bytes.
    If set to None, or to a value which doesn't make sense (ex: requesting depth = DATASET for a DID of type FILE)
    will not compute the size dynamically.
    :param session: The database session in use.
    """

    if did.did_type == DIDType.DATASET and dynamic_depth == DIDType.FILE or \
            did.did_type == DIDType.CONTAINER and dynamic_depth in (DIDType.FILE, DIDType.DATASET):

        if did.did_type == DIDType.DATASET and dynamic_depth == DIDType.FILE:
            stmt = select(
                func.count(),
                func.sum(models.DataIdentifierAssociation.bytes),
                func.sum(models.DataIdentifierAssociation.events),
            ).where(
                and_(models.DataIdentifierAssociation.scope == did.scope,
                     models.DataIdentifierAssociation.name == did.name)
            )
        elif did.did_type == DIDType.CONTAINER and dynamic_depth == DIDType.DATASET:
            child_did_stmt = list_one_did_childs_stmt(did.scope, did.name, did_type=DIDType.DATASET).subquery()
            stmt = select(
                func.sum(models.DataIdentifier.length),
                func.sum(models.DataIdentifier.bytes),
                func.sum(models.DataIdentifier.events),
            ).join_from(
                child_did_stmt,
                models.DataIdentifier,
                and_(models.DataIdentifier.scope == child_did_stmt.c.scope,
                     models.DataIdentifier.name == child_did_stmt.c.name),
            )
        else:  # did.did_type == DIDType.CONTAINER and dynamic_depth == DIDType.FILE:
            child_did_stmt = list_one_did_childs_stmt(did.scope, did.name, did_type=DIDType.DATASET).subquery()
            stmt = select(
                func.count(),
                func.sum(models.DataIdentifierAssociation.bytes),
                func.sum(models.DataIdentifierAssociation.events),
            ).join_from(
                child_did_stmt,
                models.DataIdentifierAssociation,
                and_(models.DataIdentifierAssociation.scope == child_did_stmt.c.scope,
                     models.DataIdentifierAssociation.name == child_did_stmt.c.name)
            )

        try:
            length, bytes_, events = session.execute(stmt).one()
            length = length or 0
            bytes_ = bytes_ or 0
            events = events or 0
        except NoResultFound:
            bytes_, length, events = 0, 0, 0
    elif did.did_type == DIDType.FILE:
        bytes_, length, events = did.bytes or 0, 1, did.events or 0
    else:
        bytes_, length, events = did.bytes or 0, did.length or 0, did.events or 0
    return bytes_, length, events


@transactional_session
def resurrect(dids: "Iterable[Mapping[str, Any]]", *, session: "Session") -> None:
    """
    Resurrect data identifiers.

    :param dids: The list of DIDs to resurrect.
    :param session: The database session in use.
    """
    for did in dids:
        try:
            stmt = select(
                models.DeletedDataIdentifier
            ).with_hint(
                models.DeletedDataIdentifier,
                'INDEX(DELETED_DIDS DELETED_DIDS_PK)',
                'oracle'
            ).where(
                and_(models.DeletedDataIdentifier.scope == did['scope'],
                     models.DeletedDataIdentifier.name == did['name'])
            )
            del_did = session.execute(stmt).scalar_one()
        except NoResultFound:
            # Dataset might still exist, but could have an expiration date, if it has, remove it
            stmt = update(
                models.DataIdentifier
            ).where(
                and_(models.DataIdentifier.scope == did['scope'],
                     models.DataIdentifier.name == did['name'],
                     models.DataIdentifier.expired_at < datetime.utcnow())
            ).values({
                models.DataIdentifier.expired_at: None
            }).execution_options(
                synchronize_session=False
            )
            rowcount = session.execute(stmt).rowcount
            if rowcount:
                continue
            raise exception.DataIdentifierNotFound("Deleted Data identifier '%(scope)s:%(name)s' not found" % did)

        # Check did_type
        # if del_did.did_type  == DIDType.FILE:
        #    raise exception.UnsupportedOperation("File '%(scope)s:%(name)s' cannot be resurrected" % did)

        kargs = del_did.to_dict()
        if kargs['expired_at']:
            kargs['expired_at'] = None

        stmt = delete(
            models.DeletedDataIdentifier
        ).prefix_with(
            "/*+ INDEX(DELETED_DIDS DELETED_DIDS_PK) */", dialect='oracle'
        ).where(
            and_(models.DeletedDataIdentifier.scope == did['scope'],
                 models.DeletedDataIdentifier.name == did['name'])
        )
        session.execute(stmt)

        models.DataIdentifier(**kargs).save(session=session, flush=False)


@stream_session
def list_archive_content(
    scope: "InternalScope",
    name,
    *,
    session: "Session"
) -> "Iterator[dict[str, Any]]":
    """
    List archive contents.

    :param scope: The archive scope name.
    :param name: The archive data identifier name.
    :param session: The database session in use.
    """
    try:
        stmt = select(
            models.ConstituentAssociation
        ).with_hint(
            models.ConstituentAssociation,
            'INDEX(ARCHIVE_CONTENTS ARCH_CONTENTS_PK)',
            'oracle'
        ).where(
            and_(models.ConstituentAssociation.scope == scope,
                 models.ConstituentAssociation.name == name)
        )

        for tmp_did in session.execute(stmt).yield_per(5).scalars():
            yield {'scope': tmp_did.child_scope, 'name': tmp_did.child_name,
                   'bytes': tmp_did.bytes, 'adler32': tmp_did.adler32, 'md5': tmp_did.md5}
    except NoResultFound:
        raise exception.DataIdentifierNotFound(f"Data identifier '{scope}:{name}' not found")


@transactional_session
def add_did_to_followed(
    scope: "InternalScope",
    name: str,
    account: "InternalAccount",
    *,
    session: "Session"
) -> None:
    """
    Mark a DID as followed by the given account

    :param scope: The scope name.
    :param name: The data identifier name.
    :param account: The account owner.
    :param session: The database session in use.
    """
    return add_dids_to_followed(dids=[{'scope': scope, 'name': name}],
                                account=account, session=session)


@transactional_session
def add_dids_to_followed(
    dids: "Iterable[Mapping[str, Any]]",
    account: "InternalAccount",
    *,
    session: "Session"
) -> None:
    """
    Bulk mark datasets as followed

    :param dids: A list of DIDs.
    :param account: The account owner.
    :param session: The database session in use.
    """
    try:
        for did in dids:
            # Get the DID details corresponding to the scope and name passed.
            stmt = select(
                models.DataIdentifier
            ).where(
                and_(models.DataIdentifier.scope == did['scope'],
                     models.DataIdentifier.name == did['name'])
            )
            did = session.execute(stmt).scalar_one()
            # Add the queried to the followed table.
            new_did_followed = models.DidFollowed(scope=did.scope, name=did.name, account=account,
                                                  did_type=did.did_type)

            new_did_followed.save(session=session, flush=False)

        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@stream_session
def get_users_following_did(
    scope: "InternalScope",
    name: str,
    *,
    session: "Session"
) -> "Iterator[dict[str, InternalAccount]]":
    """
    Return list of users following a DID

    :param scope: The scope name.
    :param name: The data identifier name.
    :param session: The database session in use.
    """
    try:
        stmt = select(
            models.DidFollowed
        ).where(
            and_(models.DidFollowed.scope == scope,
                 models.DidFollowed.name == name)
        )
        for user in session.execute(stmt).scalars().all():
            # Return a dictionary of users to be rendered as json.
            yield {'user': user.account}

    except NoResultFound:
        raise exception.DataIdentifierNotFound("Data identifier '%s:%s' not found" % (scope, name))


@transactional_session
def remove_did_from_followed(
    scope: "InternalScope",
    name: str,
    account: "InternalAccount",
    *,
    session: "Session"
) -> None:
    """
    Mark a DID as not followed

    :param scope: The scope name.
    :param name: The data identifier name.
    :param account: The account owner.
    :param session: The database session in use.
    """
    return remove_dids_from_followed(dids=[{'scope': scope, 'name': name}],
                                     account=account, session=session)


@transactional_session
def remove_dids_from_followed(
    dids: "Iterable[Mapping[str, Any]]",
    account: "InternalAccount",
    *,
    session: "Session"
) -> None:
    """
    Bulk mark datasets as not followed

    :param dids: A list of DIDs.
    :param account: The account owner.
    :param session: The database session in use.
    """
    try:
        for did in dids:
            stmt = delete(
                models.DidFollowed
            ).where(
                and_(models.DidFollowed.scope == did['scope'],
                     models.DidFollowed.name == did['name'],
                     models.DidFollowed.account == account)
            ).execution_options(
                synchronize_session=False
            )
            session.execute(stmt)
    except NoResultFound:
        raise exception.DataIdentifierNotFound("Data identifier '%s:%s' not found" % (did['scope'], did['name']))


@transactional_session
def trigger_event(
    scope: "InternalScope",
    name: str,
    event_type: str,
    payload: str,
    *,
    session: "Session"
) -> None:
    """
    Records changes occurring in the DID to the FollowEvent table

    :param scope: The scope name.
    :param name: The data identifier name.
    :param event_type: The type of event affecting the DID.
    :param payload: Any message to be stored along with the event.
    :param session: The database session in use.
    """
    try:
        stmt = select(
            models.DidFollowed
        ).where(
            and_(models.DidFollowed.scope == scope,
                 models.DidFollowed.name == name)
        )
        for did in session.execute(stmt).scalars().all():
            # Create a new event using the specified parameters.
            new_event = models.FollowEvent(scope=scope, name=name, account=did.account,
                                           did_type=did.did_type, event_type=event_type, payload=payload)
            new_event.save(session=session, flush=False)

        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@read_session
def create_reports(
    total_workers: int,
    worker_number: int,
    *,
    session: "Session"
) -> None:
    """
    Create a summary report of the events affecting a dataset, for its followers.

    :param session: The database session in use.
    """
    # Query the FollowEvent table
    stmt = select(
        models.FollowEvent
    ).order_by(
        models.FollowEvent.created_at
    )

    # Use heartbeat mechanism to select a chunk of events based on the hashed account
    stmt = filter_thread_work(session=session, query=stmt, total_threads=total_workers, thread_id=worker_number, hash_variable='account')

    try:
        events = session.execute(stmt).scalars().all()
        # If events exist for an account then create a report.
        if events:
            body = '''
                Hello,
                This is an auto-generated report of the events that have affected the datasets you follow.

                '''
            account = None
            for i, event in enumerate(events):
                # Add each event to the message body.
                body += "{}. Dataset: {} Event: {}\n".format(i + 1, event.name, event.event_type)
                if event.payload:
                    body += "Message: {}\n".format(event.payload)
                body += "\n"
                account = event.account
                # Clean up the event after creating the report
                stmt = delete(
                    models.FollowEvent
                ).where(
                    and_(models.FollowEvent.scope == event.scope,
                         models.FollowEvent.name == event.name,
                         models.FollowEvent.account == event.account)
                ).execution_options(
                    synchronize_session=False
                )
                session.execute(stmt)

            body += "Thank You."
            # Get the email associated with the account.
            stmt = select(
                models.Account.email
            ).where(
                models.Account.account == account
            )
            email = session.execute(stmt).scalar()
            add_message('email', {'to': email,
                                  'subject': 'Report of affected dataset(s)',
                                  'body': body})

    except NoResultFound:
        raise exception.AccountNotFound("No email found for given account.")


@transactional_session
def insert_content_history(
    filter_: "ColumnExpressionArgument[bool]",
    did_created_at: datetime,
    *,
    session: "Session"
) -> None:
    """
    Insert into content history a list of DID

    :param filter_: Content clause of the files to archive
    :param did_created_at: Creation date of the DID
    :param session: The database session in use.
    """
    new_did_created_at = did_created_at
    stmt = select(
        models.DataIdentifierAssociation.scope,
        models.DataIdentifierAssociation.name,
        models.DataIdentifierAssociation.child_scope,
        models.DataIdentifierAssociation.child_name,
        models.DataIdentifierAssociation.did_type,
        models.DataIdentifierAssociation.child_type,
        models.DataIdentifierAssociation.bytes,
        models.DataIdentifierAssociation.adler32,
        models.DataIdentifierAssociation.md5,
        models.DataIdentifierAssociation.guid,
        models.DataIdentifierAssociation.events,
        models.DataIdentifierAssociation.rule_evaluation,
        models.DataIdentifierAssociation.created_at,
        models.DataIdentifierAssociation.updated_at
    ).where(
        filter_
    )
    for cont in session.execute(stmt).all():
        if not did_created_at:
            new_did_created_at = cont.created_at
        models.DataIdentifierAssociationHistory(
            scope=cont.scope,
            name=cont.name,
            child_scope=cont.child_scope,
            child_name=cont.child_name,
            did_type=cont.did_type,
            child_type=cont.child_type,
            bytes=cont.bytes,
            adler32=cont.adler32,
            md5=cont.md5,
            guid=cont.guid,
            events=cont.events,
            rule_evaluation=cont.rule_evaluation,
            updated_at=cont.updated_at,
            created_at=cont.created_at,
            did_created_at=new_did_created_at,
            deleted_at=datetime.utcnow()
        ).save(session=session, flush=False)


@transactional_session
def insert_deleted_dids(filter_: "ColumnExpressionArgument[bool]", *, session: "Session") -> None:
    """
    Insert into deleted_dids a list of did

    :param filter_: The database filter to retrieve DIDs for archival
    :param session: The database session in use.
    """
    stmt = select(
        models.DataIdentifier.scope,
        models.DataIdentifier.name,
        models.DataIdentifier.account,
        models.DataIdentifier.did_type,
        models.DataIdentifier.is_open,
        models.DataIdentifier.monotonic,
        models.DataIdentifier.hidden,
        models.DataIdentifier.obsolete,
        models.DataIdentifier.complete,
        models.DataIdentifier.is_new,
        models.DataIdentifier.availability,
        models.DataIdentifier.suppressed,
        models.DataIdentifier.bytes,
        models.DataIdentifier.length,
        models.DataIdentifier.md5,
        models.DataIdentifier.adler32,
        models.DataIdentifier.expired_at,
        models.DataIdentifier.purge_replicas,
        models.DataIdentifier.deleted_at,
        models.DataIdentifier.events,
        models.DataIdentifier.guid,
        models.DataIdentifier.project,
        models.DataIdentifier.datatype,
        models.DataIdentifier.run_number,
        models.DataIdentifier.stream_name,
        models.DataIdentifier.prod_step,
        models.DataIdentifier.version,
        models.DataIdentifier.campaign,
        models.DataIdentifier.task_id,
        models.DataIdentifier.panda_id,
        models.DataIdentifier.lumiblocknr,
        models.DataIdentifier.provenance,
        models.DataIdentifier.phys_group,
        models.DataIdentifier.transient,
        models.DataIdentifier.accessed_at,
        models.DataIdentifier.closed_at,
        models.DataIdentifier.eol_at,
        models.DataIdentifier.is_archive,
        models.DataIdentifier.constituent,
        models.DataIdentifier.access_cnt
    ).where(
        filter_
    )

    for did in session.execute(stmt).all():
        models.DeletedDataIdentifier(
            scope=did.scope,
            name=did.name,
            account=did.account,
            did_type=did.did_type,
            is_open=did.is_open,
            monotonic=did.monotonic,
            hidden=did.hidden,
            obsolete=did.obsolete,
            complete=did.complete,
            is_new=did.is_new,
            availability=did.availability,
            suppressed=did.suppressed,
            bytes=did.bytes,
            length=did.length,
            md5=did.md5,
            adler32=did.adler32,
            expired_at=did.expired_at,
            purge_replicas=did.purge_replicas,
            deleted_at=datetime.utcnow(),
            events=did.events,
            guid=did.guid,
            project=did.project,
            datatype=did.datatype,
            run_number=did.run_number,
            stream_name=did.stream_name,
            prod_step=did.prod_step,
            version=did.version,
            campaign=did.campaign,
            task_id=did.task_id,
            panda_id=did.panda_id,
            lumiblocknr=did.lumiblocknr,
            provenance=did.provenance,
            phys_group=did.phys_group,
            transient=did.transient,
            accessed_at=did.accessed_at,
            closed_at=did.closed_at,
            eol_at=did.eol_at,
            is_archive=did.is_archive,
            constituent=did.constituent
        ).save(session=session, flush=False)
