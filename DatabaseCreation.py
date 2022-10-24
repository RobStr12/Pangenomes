from typedb.client import TypeDB, SessionType, TransactionType

path = "./PhoneCallScheme.tql"

with TypeDB.core_client("localhost:1729") as client:
    exists = client.databases().contains("Social_Network")
    if exists:
        client.databases().get("Social_Network").delete()
    client.databases().create("Social_Network")
    with client.session("Social_Network", SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.WRITE) as transaction:
            with open(path, "r") as data:
                query = data.read()
                transaction.query().define(query)
                transaction.commit()
