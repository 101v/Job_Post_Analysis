'''
    Manages twitter streaming rules for the account
'''
import logging

import requests
import twitter_auth.bearer_token as bearer_token
import twitter_urls

_max_rules_allowed = 5
_max_rule_length = 512

logger = logging.getLogger(__name__)


def commit_filter_rules_if_required():
    logger.info("Checking filter rules")
    effective_rules = get_rule_list_from_file("rules/effective_rules.txt")
    already_commited_rules = get_rule_list_from_twitter_account()
    print("--------- Existing filter rules")
    print(already_commited_rules)
    if not rule_lists_are_same(effective_rules, already_commited_rules):
        logger.info("Rules have been changed")
        if already_commited_rules:
            delete_rules_from_twitter_account(already_commited_rules)
            logger.info("Existing rules are deleted")
        commit_rules(effective_rules)
        logger.info("New rules commited")
    else:
        logger.info("""There is no change in rules so not updating rules on
        twitter server""")


def validate_rules(rule_list):
    if not rule_list:
        raise Exception(
            "Twitter rules are empty"
        )

    if None in rule_list or "" in rule_list:
        raise Exception(
            "One or more twitter rules are empty"
        )

    if len(rule_list) > _max_rules_allowed:
        raise Exception(
            "Maximum only 5 rules are allowed"
        )

    for rule in rule_list:
        if len(rule) > _max_rule_length:
            raise Exception(
                "Rule length can not be more than 512 characters"
            )


def preserve_last_committed_rules(rule_list, file_name):
    if rule_list is None or len(rule_list) == 0:
        return

    with open(file_name, "w") as f:
        f.writelines(rule_list)


def rule_lists_are_same(effective_rules, committed_rules):
    if len(effective_rules) != len(committed_rules):
        return False

    committed_rules = [item["value"] for item in committed_rules]
    for rule in effective_rules:
        if rule not in committed_rules:
            return False

    return True


def get_rule_list_from_file(file_name):
    rule_list = []
    with open(file_name) as f:
        rule_list = f.readlines()
        rule_list = [rule.strip() for rule in rule_list]
    return rule_list


def get_rule_list_from_twitter_account():
    with requests.get(
        twitter_urls.rule_commit_endpoint,
        auth=bearer_token.get_bearer_oauth_header
    ) as response:

        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(
                    response.status_code, response.text)
                )

        rules_response = response.json()
        if rules_response is not None and len(rules_response["data"]) > 0:
            return rules_response["data"]
        return []


def commit_rules(rule_list):
    rules_to_commit = []
    for idx, rule in enumerate(rule_list):
        twitter_filter_rule = {"value": rule, "tag": "Rule " + str(idx)}
        rules_to_commit.append(twitter_filter_rule)

    payload = {"add": rules_to_commit}

    with requests.post(
        twitter_urls.rule_commit_endpoint,
        auth=bearer_token.get_bearer_oauth_header,
        json=payload
    ) as response:

        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(
                    response.status_code, response.text)
                )


def delete_rules_from_twitter_account(rules):
    if rules is None:
        return None

    ids = list(map(lambda rule: rule["id"], rules))

    payload = {"delete": {"ids": ids}}

    with requests.post(
        twitter_urls.rule_commit_endpoint,
        auth=bearer_token.get_bearer_oauth_header,
        json=payload
    ) as response:

        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text)
                )
