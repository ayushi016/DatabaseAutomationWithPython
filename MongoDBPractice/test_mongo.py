from datetime import datetime
import pytest
from pymongo import MongoClient
import unittest


class test_CollectionRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        client = MongoClient('localhost', 27017)
        # connect to databaseAutomation, it will create if not exists
        cls.session = client.demo
        cls.collection_name = 'employee'

    @classmethod
    def tearDownClass(cls) -> None:
        print("all test case run")

    def test_create(self):
        if self.collection_name not in self.session.list_collection_names():
            self.session.create_collection(self.collection_name)
            assert self.collection_name in self.session.list_collection_names()

    def test_list_all_collection(self):
        list1 = self.session.list_collection_names()
        for r in list1:
            print(r)
        assert self.collection_name in list1

    @pytest.mark.dependency(depends=["test_list_all_collection"])
    def test_delete(self):
        self.session.drop_collection(self.collection_name)
        # self.session[self.collection_name].drop()
        assert self.collection_name not in self.session.list_collection_names()



class DocumentRepository:

    def __init__(self, session):
        self.session = session
        self.table_name = "data"
        self.collection_name = 'employee'

    def insert(self):
        list_names = ["Juhi","dilip","jeevan","ram","shyam"]
        list_names = iter(list_names)
        empList = [{'ID': 1, 'Name': 'Juhi', 'age': 20, 'salary':1000, "date": datetime.utcnow()},
                   {'ID': 2, 'Name': 'dilip', 'age': 20, 'salary':3000},
                   {'ID': 3, 'Name': 'jeevan', 'age': 24, 'salary': 145}]
        self.session[self.collection_name].insert_many(empList)
        self.session[self.collection_name].insert_many([{'ID': 4, 'Name': 'ram', 'age': 27, 'salary': 145}])
        self.session[self.collection_name].insert_one({'ID': 5, 'Name': 'shyam', 'age': 37, 'salary': 1500})

        cursor = self.session[self.collection_name].find({'ID': 4})
        for record in cursor:
            test_name = record['Name']
            test_age = record['age']
            test_salary = record['salary']
            assert test_name == 'ram'
            assert test_age == 27
            assert test_salary == 145

    def show_all_documents(self):
        list_names = ["Juhi", "dilip", "jeevan", "ram", "shyam"]
        list_names = iter(list_names)
        cursor = self.session[self.collection_name].find()
        # docs = self.session[self.collection_name].find({'Name': 'Juhi'})

        for record in cursor:
            test_name = record['Name']
            print(record)
            assert test_name in list_names

    def show_selected_documents(self):
        cursor = self.session[self.collection_name].find({'age': {'$gt': 21}})
        for record in cursor:
            print(record.get('ID'), record['Name'],record['age'])
            test_age = record['age']
            assert test_age > 21

    def update(self):
        self.session[self.collection_name].update_one({'Name': 'Juhi', 'ID': 1}, {"$set": {'salary': 15000}})
        cursor = self.session[self.collection_name].find({'Name': 'Juhi', 'ID': 1})
        for record in cursor:
            print(record)
            test_salary = record['salary']
            assert test_salary == 15000

    def delete(self):
        list_names = ["Juhi", "dilip", "jeevan", "ram", "shyam"]
        list_names = iter(list_names)
        self.session[self.collection_name].delete_many({'ID': {'$gt': 3}, 'salary': {'$lt': 1000}})
        cursor = self.session[self.collection_name].find({'ID': {'$gt': 3}, 'salary': {'$lt': 1000}})
        assert cursor.count() == 0

    def count_data(self):
        n = self.session[self.collection_name].find({'salary': {'$lt': 1000}}).count()
        assert n == 1


# session = connect_to_database()
# CollectionRepository(session).create()
# CollectionRepository(session).list_all_collection()
# CollectionRepository(session).delete()
# DocumentRepository(session).insert()
# DocumentRepository(session).show_all_documents()
# DocumentRepository(session).show_selected_documents()
# DocumentRepository(session).update()
# DocumentRepository(session).delete()
# DocumentRepository(session).count_data()


