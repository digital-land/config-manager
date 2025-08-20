from flask import request, g, abort, render_template
from functools import wraps

# Placeholder imports for your actual implementations
from .common_middleware import (
    fetch_dataset_info,
    fetch_org_info,
    log_page_error,
    validate_query_params,
    create_pagination_template_params,
    show_404_if_page_number_not_in_range,
    fetch_resources,
    process_relevant_issues_middlewares,
    process_entities_middlewares,
    process_specification_middlewares,
    get_set_base_sub_path,
    get_set_data_range,
    get_error_summary_items,
    prepare_issue_details_template_params,
    filter_out_entities_without_issues,
    get_issue_specification
)
from .middleware_builders import render_template as render_template_middleware
from ..utils.errors import MiddlewareError
from ..utils.utils import issue_error_message_html

from marshmallow import Schema, fields, ValidationError

class IssueDetailsQueryParamsSchema(Schema):
    lpa = fields.String(required=True)
    dataset = fields.String(required=True)
    issue_type = fields.String(required=True)
    issue_field = fields.String(required=True)
    pageNumber = fields.Integer(missing=1)
    resourceId = fields.String(required=False)

def validate_issue_details_query_params(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        schema = IssueDetailsQueryParamsSchema()
        try:
            g.parsed_params = schema.load(request.args)
        except ValidationError as err:
            abort(400, str(err))
        return f(*args, **kwargs)
    return decorated_function

def get_issue_field(text, html, classes=None):
    classes = classes or ''
    return {
        "key": {"text": text},
        "value": {
            "html": str(html) if html else '',
            "originalValue": html
        },
        "classes": classes
    }

def set_record_count():
    g.record_count = len(getattr(g, 'issue_entities', [])) if hasattr(g, 'issue_entities') else 0

def prepare_entity():
    issue_entities = getattr(g, 'issue_entities', [])
    issues = getattr(g, 'issues', [])
    specification = getattr(g, 'specification', {})
    page_number = g.parsed_params.get('pageNumber', 1)
    issue_type = g.parsed_params.get('issue_type')

    if not issue_entities:
        raise MiddlewareError('No issues for entity', 404)

    entity_data = issue_entities[page_number - 1]
    entity_issues = [issue for issue in issues if issue['entity'] == entity_data['entity']]

    spec_fields = {}
    for field_info in specification.get('fields', []):
        field = field_info.get('field')
        dataset_field = field_info.get('datasetField')
        value = entity_data.get(field) or entity_data.get(dataset_field)
        spec_fields[field] = get_issue_field(field, value)

    for issue in entity_issues:
        field = spec_fields.get(issue['field'])
        if field:
            message = issue.get('message') or issue.get('type')
            field['value']['html'] = issue_error_message_html(message, None) + field['value']['html']
            field['classes'] += 'dl-summary-card-list__row--error govuk-form-group--error'
        else:
            error_message = issue.get('message') or issue_type
            value_html = issue_error_message_html(error_message, issue.get('value'))
            classes = 'dl-summary-card-list__row--error govuk-form-group--error'
            new_field = get_issue_field(issue['field'], value_html, classes)
            new_field['value']['originalValue'] = issue.get('value')
            spec_fields[issue['field']] = new_field

    reference = spec_fields.get('reference')
    geometries = []
    def push_geometry(key):
        val = spec_fields.get(key)
        if val:
            geometries.append({
                "type": key,
                "geo": val['value']['originalValue'],
                "reference": reference['value']['html'] if reference else None
            })
    push_geometry('geometry')
    push_geometry('point')

    g.entry = {
        "title": entity_data.get('name') or f"entity: {entity_data['entity']}",
        "fields": list(spec_fields.values()),
        "geometries": geometries
    }

def show_404_if_no_issues():
    if getattr(g, 'record_count', 0) == 0:
        raise MiddlewareError('no issues found', 404)

def get_issue_details():
    # This assumes you have set up g.template_params somewhere before
    return render_template('organisations/issueDetails.html', **g.template_params)

# Example of chaining these in a Flask route
from flask import Blueprint

bp = Blueprint('entity_issue_details', __name__)

@bp.route('/issue-details')
@validate_issue_details_query_params
def issue_details():
    fetch_org_info()
    fetch_dataset_info()
    fetch_resources()
    for mw in process_entities_middlewares:
        mw()
    for mw in process_relevant_issues_middlewares:
        mw()
    for mw in process_specification_middlewares:
        mw()
    get_issue_specification()
    filter_out_entities_without_issues()
    set_record_count()
    show_404_if_no_issues()
    get_set_data_range(1)
    show_404_if_page_number_not_in_range()
    get_set_base_sub_path(['entity'])
    create_pagination_template_params()
    get_error_summary_items()
    prepare_entity()
    prepare_issue_details_template_params()
    response = get_issue_details()
    log_page_error()
    return response