from typedb.client import TypeDB, SessionType, TransactionType

with TypeDB.core_client("localhost:1729") as client:
    exists = client.databases().contains("Social Network")
    if exists is False:
        client.databases().create("Social Network")
        with client.session("Social Network", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as transaction:
                print("OK")
