import datetime

from flask import Blueprint, render_template

from application.data_access.overview.api_queries import get_organisation_entity
from application.data_access.overview.digital_land_queries import (
    get_grouped_source_counts,
    get_organisation_sources,
    get_organisation_stats,
    get_publishers,
    get_resource_count_per_dataset,
)
from application.data_access.overview.entity_queries import (
    get_datasets_organisation_has_used_enddates,
    get_organisation_entity_count,
)
from application.utils import index_by, index_with_list

publisher_pages = Blueprint("publisher", __name__, url_prefix="/organisation")


def publisher_info():
    # returns all publishers, even empty
    publisher_source_counts = get_publishers()
    # return just the publishers we have data for
    publisher_stats = get_organisation_stats()
    empty_stats = {"resources": 0, "active": 0, "endpoints": 0, "pipelines": 0}
    publishers = {}
    for pub, stats in publisher_source_counts.items():
        if pub in publisher_stats.keys():
            publishers[pub] = {**stats, **publisher_stats[pub]}
        else:
            publishers[pub] = {**stats, **empty_stats}
    return publishers


def split_publishers(organisations):
    lpas = {
        publisher: organisations[publisher]
        for publisher in organisations.keys()
        if "local-authority" in publisher
    }
    dev_corps = {
        publisher: organisations[publisher]
        for publisher in organisations.keys()
        if "development-corporation" in publisher
    }
    national_parks = {
        publisher: organisations[publisher]
        for publisher in organisations.keys()
        if "national-park" in publisher
    }
    other = {
        publisher: organisations[publisher]
        for publisher in organisations.keys()
        if not any(
            s in publisher
            for s in ["local-authority", "development-corporation", "national-park"]
        )
    }
    return {
        "Development corporation": dev_corps,
        "National parks": national_parks,
        "Other publishers": other,
        "Local authorities": lpas,
    }


@publisher_pages.route("/")
def organisation():
    publishers = publisher_info()
    active_publishers = {
        k: publisher
        for k, publisher in publishers.items()
        if publisher["sources_with_endpoint"] > 0
    }
    publishers_with_no_data = {
        k: publisher
        for k, publisher in publishers.items()
        if publisher["sources_with_endpoint"] == 0
    }

    return render_template(
        "organisation/index.html",
        publishers=split_publishers(active_publishers),
        today=datetime.datetime.utcnow().isoformat()[:10],
        none_publishers=split_publishers(publishers_with_no_data),
    )


@publisher_pages.route("/<prefix>/<org_id>")
def organisation_performance(prefix, org_id):
    id = prefix + ":" + org_id
    organisation = get_organisation_entity(prefix, org_id)
    resource_counts = get_resource_count_per_dataset(id)
    source_counts = get_grouped_source_counts(id)
    sources = index_with_list("pipeline", get_organisation_sources(id))

    missing_datasets = [
        dataset for dataset in source_counts if dataset["sources_with_endpoint"] == 0
    ]

    data = {"datasets": index_by("pipeline", resource_counts)}
    data["total_resources"] = sum(
        [data["datasets"][d]["resources"] for d in data["datasets"].keys()]
    )

    # TO FIX: I'm not sure this is working
    erroneous_sources = []
    for dataset in data["datasets"].keys():
        for source in sources[dataset]:
            if source["endpoint"] == "":
                erroneous_sources.append(source)

    # setup dict to capture datasets with data from secondary sources
    data["data_from_secondary"] = {}

    # add entity counts to dataset data
    entity_counts = get_organisation_entity_count(organisation=id)
    for dn, count in entity_counts.items():
        if dn in data["datasets"].keys():
            data["datasets"][dn]["entity_count"] = count
        else:
            # add dataset to list from secondary sources
            data["data_from_secondary"].setdefault(dn, {"pipeline": dn})
            data["data_from_secondary"][dn]["entity_count"] = count

    return render_template(
        "organisation/performance.html",
        organisation=organisation[0],
        data=data,
        sources_per_dataset=source_counts,
        missing_datasets=missing_datasets,
        enddates=get_datasets_organisation_has_used_enddates(id),
        erroneous_sources=erroneous_sources,
        entity_counts=entity_counts,
    )
