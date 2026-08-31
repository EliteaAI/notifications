"""Unit tests for the backfilled index notification prose in tasks/db_tasks.py.

This builder is a twin of elitea_core's ``utils/indexing_report.py``; the expected
strings here must match that module's output verbatim.
"""
import json


def make_report(**totals_overrides):
    totals = {
        'indexed': 179, 'skipped': 0, 'not_indexed': 0, 'failed': 0,
        'unchanged': 0, 'dependent_not_indexed': 0, 'total': 179,
    }
    totals.update(totals_overrides)
    return {
        'version': 1,
        'status': 'ok',
        'operation': 'reindex',
        'item_labels': {'singular': 'page', 'plural': 'pages'},
        'dependent_labels': {'singular': 'attachment', 'plural': 'attachments'},
        'totals': totals,
    }


class TestPartlyIndexedMessage:
    def test_a_known_failure_count_is_named_with_its_noun(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 4321, 'report': make_report(failed=3)}
        )

        assert message == (
            'Index [docs]() was partially reindexed: 3 pages could not be updated'
            ' (boom). Their previously indexed data remains available for search.'
        )

    def test_a_single_failure_reads_in_the_singular(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 4321, 'report': make_report(failed=1)}
        )

        assert message == (
            'Index [docs]() was partially reindexed: 1 page could not be updated'
            ' (boom). Their previously indexed data remains available for search.'
        )

    def test_a_missing_failure_count_is_omitted_never_rendered_as_zero(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 4321}
        )

        assert message == (
            'Index [docs]() was partially reindexed: some documents could not be updated'
            ' (boom). Their previously indexed data remains available for search.'
        )
        assert '0 documents' not in message

    def test_a_first_run_makes_no_retention_claim_and_is_not_called_a_reindex(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'report': make_report(failed=2)}
        )

        assert message == (
            'Index [docs]() was partially indexed: 2 pages could not be updated (boom).'
        )

    def test_a_first_run_never_claims_retention_for_items_it_never_indexed(self, db_tasks):
        """The current run's own chunks satisfy the count, so the count alone cannot carry
        a clause about the FAILED items' earlier generation."""
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'indexed_chunks': 176, 'report': make_report(failed=2)}
        )

        assert message == (
            'Index [docs]() was partially indexed: 2 pages could not be updated (boom).'
        )

    def test_a_reindex_over_an_emptied_index_makes_no_retention_claim(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 0, 'report': make_report(failed=2)}
        )

        assert message == (
            'Index [docs]() was partially reindexed: 2 pages could not be updated (boom).'
        )

    def test_a_stringified_report_is_parsed(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 7, 'report': json.dumps(make_report(failed=5))}
        )

        assert message.startswith('Index [docs]() was partially reindexed: 5 pages')


    def test_a_damaged_doc_run_reporting_no_failed_group_is_not_rendered_as_zero(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'partly_indexed', 'error': 'boom',
             'reindex': True, 'indexed_chunks': 4321, 'report': make_report(failed=0)}
        )

        assert message.startswith('Index [docs]() was partially reindexed: some pages')


class TestFailedMessage:
    def test_an_error_already_claiming_retention_states_it_once(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'failed', 'reindex': True, 'indexed_chunks': 4321,
             'error': 'Indexing failed: the loader returned no content while the index holds'
                      ' data from previous runs.'
                      ' Previously indexed data remains available for search.'}
        )

        assert message == (
            'Index [docs]() reindex failed: Indexing failed: the loader returned no content'
            ' while the index holds data from previous runs.'
            ' Previously indexed data remains available for search.'
        )
        assert message.count('remains available for search') == 1
        assert '..' not in message

    def test_an_error_claiming_retention_over_an_emptied_index_drops_the_claim(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'failed', 'reindex': True, 'indexed_chunks': 0,
             'error': 'Loader empty. Previously indexed data remains available for search.'}
        )

        assert message == 'Index [docs]() reindex failed: Loader empty.'

    def test_a_trailing_period_is_never_doubled(self, db_tasks):
        message = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'failed', 'error': 'boom.'}
        )

        assert message == 'Indexing of [docs]() failed: boom.'

    def test_retention_is_claimed_only_with_live_chunks(self, db_tasks):
        retained = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'failed', 'error': 'boom', 'reindex': True,
             'indexed_chunks': 12}
        )
        empty = db_tasks._build_index_data_message(
            {'index_name': 'docs', 'state': 'failed', 'error': 'boom', 'reindex': True,
             'indexed_chunks': 0}
        )

        assert retained == (
            'Index [docs]() reindex failed: boom.'
            ' Previously indexed data remains available for search.'
        )
        assert empty == 'Index [docs]() reindex failed: boom.'
