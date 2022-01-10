from neo4j import GraphDatabase, basic_auth
import socket


class Movie_queries(object):
    def __init__(self, password):
        self.driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", password), encrypted=False)
        self.session = self.driver.session()
        self.transaction = self.session.begin_transaction()

    def q0(self):
        result = self.transaction.run("""
            MATCH (n:Actor) RETURN n.name, n.id ORDER BY n.birthday ASC LIMIT 3
        """)
        return [(r[0], r[1]) for r in result]
 
    def q1(self):
        result = self.transaction.run(""" 
            MATCH (p:Person)-[:ACTS_IN]->(m:Movie)  
            RETURN p.name, count(*) 
            ORDER BY count(*) DESC, p.name ASC 
            LIMIT 20 
        """)
        return [(r[0], r[1]) for r in result]

    def q2(self):  
        result = self.transaction.run("""
            MATCH (p:Person)-[:RATED]->(m:Movie)-[:ACTS_IN]-(b:Actor)
            RETURN m.title, count(distinct b) 
            ORDER BY count(distinct b) DESC 
            LIMIT 1 
        """)
        return [(r[0], r[1]) for r in result] 

    def q3(self): 
        result = self.transaction.run("""
            MATCH (p:Person)-[:DIRECTED]->(m:Movie)
            WITH p, count(distinct m.genre) as g
            WHERE g > 1
            RETURN p.name, g
            ORDER BY g DESC, p.name ASC
        """)
        return [(r[0], r[1]) for r in result]

    def q4(self):
        result = self.transaction.run("""  
           MATCH (p0:Actor{name: "Kevin Bacon"})-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(p1:Actor)
           MATCH (p1:Actor)-[:ACTS_IN]->(m1:Movie)<-[:ACTS_IN]-(p2:Actor)
           WHERE p2 <> p0 AND NOT (p0)-[:ACTS_IN]->()<-[:ACTS_IN]-(p2) 
           RETURN DISTINCT p2.name 
           ORDER BY p2.name  
        """)
        return [(r[0]) for r in result]

if __name__ == "__main__":
    sol = Movie_queries("neo4jpass")
    print("---------- Q0 ----------")
    print(sol.q0())
    print("---------- Q1 ----------")
    print(sol.q1())
    print("---------- Q2 ----------")
    print(sol.q2())
    print("---------- Q3 ----------")
    print(sol.q3())
    print("---------- Q4 ----------")
    print(sol.q4())
    sol.transaction.close()
    sol.session.close()
    sol.driver.close()
