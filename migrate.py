from typedb.client import TypeDB, SessionType, TransactionType
import csv


def company_template(company):
    return 'insert $company isa company, has name "' + company["name"] + '";'


def person_template(person):
    typeql_insert_query = 'insert $person isa person, has phone-number "' + person["phone_number"] + '"'

    if person["first_name"] == "":
        typeql_insert_query += ', has is-customer false'
    else:
        typeql_insert_query += ", has is-customer true"
        typeql_insert_query += ', has first-name "' + person["first_name"] + '"'
        typeql_insert_query += ', has last-name "' + person["last_name"] + '"'
        typeql_insert_query += ', has city "' + person["city"] + '"'
        typeql_insert_query += ', has age "' + str(person["age"]) + '"'
    typeql_insert_query += ";"
    return typeql_insert_query


def contract_template(contract):
    typeql_insert_query = 'match $company isa company, has name "' + contract["company_name"] + '";'
    typeql_insert_query += ' $customer isa person, has phone-number "' + contract["person_id"] + '";'
    typeql_insert_query += ' insert (provider: $company, customer: $customer) isa contract;'
    return typeql_insert_query


def call_template(call):
    typeql_insert_query = 'match $caller isa person, has name "' + call["caller_id"] + '";'
    typeql_insert_query += ' $callee isa person, has name "' + call["callee_id"] + '";'
    typeql_insert_query += ' insert $call(caller: $caller, callee: $callee) isa call; $call has started-at ' + call["started_at"] +'; $call has duration ' + str(call["duration"]) + ';'
    return typeql_insert_query


def parse_data_to_dictionaries(input):
    items = []
    with open(input["data_path"] + ".csv") as data:
        for row in csv.DictReader(data, skipinitialspace=True):
            item = {key: value for key, value in row.items()}
            items.append(item)
    return items


def load_data_into_typedb(input, session):
    items = parse_data_to_dictionaries(input)

    with session.transaction(TransactionType.WRITE) as transaction:
        for item in items:
            typeql_insert_query = input["template"](item)
            print("Executing TypeQL Query: " + typeql_insert_query)
            transaction.query().insert(typeql_insert_query)
        transaction.commit()
    
    print("\nInsrted" + str(len(items)) + " items from [" + input["data_path"] + "] into TypeDB.\n")


def build_phone_call_graph(inputs):
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("phone_calls", SessionType.DATA) as session:
            for input in inputs:
                print("loading from [" + input["data_path"] + "] into TypeDB ...")
                load_data_into_typedb(input, session)


inputs = [

    {"data_path": "./data/companies", "template": company_template},
    {"data_path": "./data/people", "template": person_template},
    {"data_path": "./data/contracts", "template": contract_template},
    {"data_path": "./data/calls", "template": call_template}
]
build_phone_call_graph(inputs)