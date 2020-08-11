from cassandra.cluster import Cluster, ExecutionProfile


def connect_to_database():
    execution_profile = ExecutionProfile(request_timeout=3600000)
    profile = {'node1': execution_profile}
    cluster = Cluster(['127.0.0.1'], port=9042, execution_profiles=profile)
    # cluster = Cluster()

    global session
    # self.session = self.cluster.connect()
    session = cluster.connect(keyspace='people')
    # session.set_keyspace("people")
    return session


def close_connection(session):
    session.shutdown()


class KeyspaceRepository:

    def __init__(self, session):
        self.session = session
        self.keyspace_name = "demo"

    def create(self):
        self.session.execute("Create keyspace IF NOT EXISTS " + self.keyspace_name +
                             " with replication={'class':'SimpleStrategy', 'replication_factor':1};")

        rows = self.session.execute("SELECT * FROM system_schema.keyspaces")
        print(10*"-----")
        keyspace_names =[]
        for re in rows:
            keyspace_names.append(re[0])

        print(keyspace_names)
        keyspace_names = iter(keyspace_names)
        assert self.keyspace_name in keyspace_names

    def delete(self):
        try:
            self.session.execute("DROP KEYSPACE IF EXISTS demo")
        except Exception:
            print(10 * "_____")
            rows = self.session.execute("SELECT * FROM system_schema.keyspaces")
            keyspace_names = []
            for re in rows:
                keyspace_names.append(re[0])

            print(keyspace_names)
            keyspace_names = iter(keyspace_names)
            assert self.keyspace_name not in keyspace_names


class TableRepository:

    def __init__(self,session):
        self.session = session
        self.table_name = "data"

    def create(self):
        self.session.execute("CREATE TABLE IF NOT EXISTS demo.data(id int primary key, name text, salary int)")

    def insert_data(self):
        self.session.execute("INSERT INTO demo.data(id,name,salary) VALUES(1,'xyzzz',1000);")
        stmt = self.session.prepare("INSERT INTO demo.data(id,name,salary) VALUES(?,?,?);")
        qry = stmt.bind([2, 'Ram', 23175])
        self.session.execute(qry)
        self.session.execute(stmt, [3, 'shyam', 5000])
        self.session.execute(stmt, (4, 'ABCC', 15000))
        print(10*"---")
        rows = self.session.execute("SELECT * FROM demo.data where id=1 ALLOW FILTERING")
        print(rows[0])
        assert rows[0][1] == "xyzzz"
        assert rows[0][2] == 1000

    def show_table_data(self):
        print(10*"---")
        rows = self.session.execute("SELECT * FROM demo.data").all()
        print(rows)
        for record in rows:
            assert isinstance(record[0],int) == True

    def delete(self):
        try:
            self.session.execute("DROP TABLE IF EXISTS demo.data")

        except Exception:
            print("timed out")


session = connect_to_database()
KeyspaceRepository(session).create()
TableRepository(session).create()
TableRepository(session).insert_data()
TableRepository(session).show_table_data()
TableRepository(session).delete()
KeyspaceRepository(session).delete()
close_connection(session)

