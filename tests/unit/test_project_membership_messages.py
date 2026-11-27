def test_added_project_membership_message_uses_the_stored_name(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'project_name': 'Research [Archive]'},
    ) == 'You have been added to Research [Archive] project.'


def test_added_project_membership_message_includes_assigned_roles(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'project_name': 'Research', 'roles': ['viewer']},
    ) == 'You have been added to Research project as Viewer.'
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'project_name': 'Research', 'roles': ['admin', 'editor']},
    ) == 'You have been added to Research project as Admin, Editor.'


def test_added_project_membership_message_ignores_empty_or_invalid_roles(db_tasks):
    for roles in ([], [''], None, 'admin', [1]):
        assert db_tasks._build_message_for_row(
            'user_was_added_to_some_project_as_teammate',
            {'project_name': 'Research', 'roles': roles},
        ) == 'You have been added to Research project.'


def test_added_project_membership_message_keeps_legacy_metadata_without_snapshot(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'users': ['Alex']},
    ) == 'Alex is added into project.'


def test_added_project_membership_message_keeps_legacy_users_with_a_project_name(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_added_to_some_project_as_teammate',
        {'users': ['Alex'], 'project_name': 'P'},
    ) == 'Alex is added into P.'


def test_removed_project_membership_message_uses_the_stored_name(db_tasks):
    assert db_tasks._build_message_for_row(
        'user_was_removed_from_project',
        {'project_name': 'Research [Archive]'},
    ) == 'You have been removed from Research [Archive] project.'


def test_removed_project_membership_message_without_snapshot_is_skipped(db_tasks):
    assert db_tasks._build_message_for_row('user_was_removed_from_project', {}) is None
