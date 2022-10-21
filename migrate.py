from typedb.client import TypeDB, SessionType, TransactionType


def company_template(company):
    return 'insert $company isa company, has name "' + company["name"] + '";'


def person_template(person):
    typeql_insert_query = 'insert $person isa person, has phone-number "' + person["phone_number"] + '"'

    if "first_name" in person:
        typeql_insert_query += ", has is-customer true"
        typeql_insert_query += ', has first-name "' + person["first_name"] + '"'
        typeql_insert_query += ', has last-name "' + person["last_name"] + '"'
        typeql_insert_query += ', has city "' + person["city"] + '"'
        typeql_insert_query += ', has age "' + person["age"] + '"'
    else:
        typeql_insert_query += ', has is-customer false'
    return typeql_insert_query + ';'


def contract_template(contract):
    typeql_insert_query = 'match $company isa company, has name "' + contract["company_name"] + '";'
    typeql_insert_query += ' $customer isa person, has phone-number "' + contract["person_id"] + '";'
    typeql_insert_query += ' insert (provider: $company, customer: $customer) isa contract;'
    return typeql_insert_query


def call_template(call):
    pass


inputs = [
    {
        "data_path": "files/PangenomesTut/data/companies",
        "template": company_template
    },
    {
        "data_path": "files/PangenomesTut/data/people",
        "template": person_template
    },
    {
        "data_path": "files/PangenomesTut/data/contracts",
        "template": contract_template
    },
    {
        "data_path": "files/PangenomesTut/data/calls",
        "template": call_template
    }
]





def parse_data_to_dictionaries(input):
    pass


def load_data_into_typedb(input, session):
    items = parse_data_to_dictionaries(input)
    
    for item in items:
        with session.transaction(TransactionType.WRITE) as transaction:
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
