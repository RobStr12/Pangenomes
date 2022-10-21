from typedb.client import TypeDB, SessionType, TransactionType

with TypeDB.core_client("localhost:1729") as client:
    with client.session("phone_calls", SessionType.DATA) as session:
        with session.transaction(TransactionType.READ) as transaction:
            query = [
                'match',
                ' $customer isa person, has phone-number $phone-number;',
                ' $company isa company, has name "Telecom";',
                ' (customer: $customer, provider: $company) isa contract;',
                ' $target isa person, has phone-number "+86 921 547 9004";',
                ' (caller: $customer, callee: $target) isa call, has started-at $started-at;',
                ' $min-date 2018-09-14T17:18:49; $started-at > $min-date;',
                'get $phone-number;'
            ]

            print("\nQuery:\n", "\n".join(query))
            query = "".join(query)

            iterator = transaction.query().match(query)
            answers = [ans.get("phone-number").get_value() for ans in iterator]

            print("\nResults:\n", answers)
