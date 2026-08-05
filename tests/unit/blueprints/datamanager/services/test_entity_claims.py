from application.blueprints.datamanager.services.entity_claims import (
    claim_entities,
    entity_clashes,
    release_claims,
    release_others,
)


class TestEntityClaims:
    def test_same_request_does_not_self_clash(self, app):
        with app.app_context():
            assert claim_entities("lp-a", "config-manager-update", "req-a", [100, 101])
            # the owning request sees no clash on its own numbers
            assert (
                entity_clashes("lp-a", "config-manager-update", "req-a", [100, 101]) == []
            )
            # a different request sees the overlap
            assert entity_clashes(
                "lp-a", "config-manager-update", "req-b", [100, 101]
            ) == [100, 101]

    def test_claims_are_branch_scoped(self, app):
        with app.app_context():
            claim_entities("lp-b", "config-manager-update", "req-c", [200])
            # same number on a different branch is not a clash
            assert (
                entity_clashes("lp-b", "test-config-manager-update", "req-d", [200]) == []
            )

    def test_concurrent_claim_conflict_returns_false(self, app):
        with app.app_context():
            assert claim_entities("lp-c", "config-manager-update", "req-e", [300])
            # another request claiming the same number hits the unique constraint
            assert not claim_entities("lp-c", "config-manager-update", "req-f", [300])

    def test_release_claims_frees_numbers(self, app):
        with app.app_context():
            claim_entities("lp-d", "config-manager-update", "req-g", [400])
            release_claims("req-g")
            assert (
                entity_clashes("lp-d", "config-manager-update", "req-h", [400]) == []
            )

    def test_release_others_frees_specific_numbers(self, app):
        with app.app_context():
            claim_entities("lp-e", "config-manager-update", "req-i", [500, 501])
            release_others("lp-e", "config-manager-update", [500])
            # 500 freed, 501 still owned by req-i
            assert entity_clashes(
                "lp-e", "config-manager-update", "req-j", [500, 501]
            ) == [501]
