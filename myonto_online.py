from __future__ import annotations
from dataclasses import replace
from typing import Type
from abc import ABC, abstractmethod
import numpy as np
import rdflib
from rdflib import Graph, URIRef
from rdflib import RDF, RDFS
from rdflib import OWL
from rdflib.plugins.sparql import prepareQuery
import utils
import time
import re
from flask import abort
import json
import requests
import sys

class MyOWLOntology():
    def __init__(self, ont_file, pr: str=None):
        self.concepts = {}  # string: owl_concept
        self._concepts = []  # URIRef
        self.individuals = {}  # string: myowl_individual
        self._individuals = []  # URIRef
        self.relations = {}  # string: wol_relation
        self._relations = []  # URIRef
        self.ancestors = {}  # owl_logical_entity: set of owl_class
        self.concept_distances = {}  # owl_class: dictionary of owl_class: integer
        self.lcas = {}  # annotation_comparison: owl_concept
        self.concept_profs = {}
        self.relation_profs = {}
        self.property_chains = {}
        self.exp_id = 0
        self.storing = True
        self.prefix = pr
        self.endpoint = 'clarify'

        self.prepared_queries = {
            'q1': '''
                    SELECT ?obj
                    WHERE {
                        ?c rdfs:subClassOf ?sc.
                        ?sc owl:someValuesFrom ?obj.
                    }  
                    ''',
            'q2': '''
                    SELECT ?c
                    WHERE {
                        ?c owl:onProperty ?p.
                        ?c owl:someValuesFrom ?b.
                    }
                    ''',
            'q3': '''
                    SELECT DISTINCT ?p ?value
                    WHERE {
                        {
                            ?p rdfs:domain ?sc.
                            ?x ?p ?value.
                            ?c rdfs:subClassOf* ?sc.
                        } UNION {
                            ?p rdfs:range ?sc.
                            ?value ?p ?x.
                            ?c rdfs:subClassOf* ?sc.
                        }
                    }
                '''
        }

        self.o = Graph()
        print('Parsing ontology...')
        if type(ont_file) == list:  # multiple graphs
            for of in ont_file:
                self.o.parse(of)
        else:
            self.o.parse(ont_file)
            # self.go = get_ontology(ont_file).load()

        # object_properties = list(set([s for s, _, _ in self.o.triples((None, RDF.type, OWL.ObjectProperty))]))
        object_properties = [rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?p a owl:ObjectProperty.
            }
            '''
            , self.endpoint
        )]
        if OWL.topObjectProperty in object_properties:
            object_properties.remove(OWL.topObjectProperty)
        for op in object_properties:
            self.relations[op.n3()[1:-1]] = OWLRelation(op, self)
        self._relations = [rel.p for rel in self.relations.values()]
        print('Relations read')
        # print(self.get_OWL_relation('http://research.tib.eu/p4-lucat/vocab/hasTreatment').similarity_neighbors(self.get_OWL_relation('http://research.tib.eu/p4-lucat/vocab/hasOncologicalTreatment')))
        # for r1 in self.relations.values():
        #     for r2 in self.relations.values():
        #         sim = r1.similarity(r2)
        #         print(r1, r2, sim)
        # utils.similarity2csv(np.array(list(self.relations.values()))[np.random.randint(len(object_properties), size=500)], np.array(list(self.relations.values()))[np.random.randint(len(object_properties), size=500)], file='results/object_properties_sim.csv', cartesian=False)

        classes = [rdflib.URIRef(v['class']) for v in QueryService.query('select distinct ?class where {?s a ?class. FILTER (regex(?class,"p4-lucat"))}', self.endpoint)]
        # classes = self.__get_all_classes()
        classes.append(OWL.Thing)
        for cl in classes:
            self.concepts[cl.n3()[1:-1]] = OWLConcept(cl, self)
        self._concepts = [con.cl for con in self.concepts.values()]
        print('Classes read')
        # print('http://research.tib.eu/p4-lucat/vocab/Patient_RT_Area', 'and', 'http://research.tib.eu/p4-lucat/vocab/Patient_RT_Intention', 'has similarity ', self.get_OWL_concept('http://research.tib.eu/p4-lucat/vocab/Patient_RT_Area').similarity_neighbors(self.get_OWL_concept('http://research.tib.eu/p4-lucat/vocab/Patient_RT_Intention')))
        # for c1, c2 in zip(list(self.concepts.values())[20:30], list(self.concepts.values())[50:60]):
        #     print(str(c1), 'and', str(c2), 'has similarity ', c1.similarity_neighbors(c2))
        # utils.similarity2csv(np.array(list(self.concepts.values()))[np.random.randint(len(classes), size=40)], np.array(list(self.concepts.values()))[np.random.randint(len(classes), size=40)], file='results/class_taxonomic_sim.csv', sim_type='similarity', cartesian=False)

        indivs = set()
        offset = 0
        while True:
            query_res = [rdflib.URIRef(v['s']) for v in QueryService.query(
                '''
                select distinct ?s
                where {
                ?s a ?o.
                    filter(!isBlank(?s))
                }
                ''' + ' offset {:d} limit 1000000'.format(int(offset))
                , self.endpoint
            )]
            if len(query_res) == 0:
                break
            indivs.update(query_res)
            offset += 1e6
            break
        indivs.difference(classes)
        indivs = list(indivs)
        for ind in indivs:
            if self.concepts.get(ind.n3()[1:-1]) is None:
                self.individuals[ind.n3()[1:-1]] = MyOWLIndividual(ind, self)
        self._individuals = [ind.ind for ind in self.individuals.values()]
        print('Individuals read')
        # for ind1, ind2 in zip(list(self.individuals.values())[30:40], list(self.individuals.values())[50:60]):
        #     print(str(ind1), 'and', str(ind2), 'has taxonomic similarity ', ind1.similarity(ind2))
        # utils.similarity2csv(np.array(list(self.individuals.values()))[np.random.randint(len(self.individuals), size=100)], np.array(list(self.individuals.values()))[np.random.randint(len(self.individuals), size=100)], sim_type='similarity', file='results/individual_sim.csv', cartesian=False)

        # ---------generate graph files for semEP-----------
        # patients = [rdflib.URIRef(v['patient']) for v in QueryService.query(
        #     '''
        #     select distinct ?patient
        #     where {
        #         ?patient a <http://research.tib.eu/p4-lucat/vocab/LCPatient>.
        #     } limit 5
        #     '''
        #     , 'tib-lucat-kg'
        # )]
        # patients = [
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient')
        # ]
        patients = [
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/1006662'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/101826'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/102008'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/104468'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/104473')
        ]
        for i, p in enumerate(patients):
            patients[i] = MyOWLIndividual(p, self)
        
        # c_trials = [rdflib.URIRef(v['clinical']) for v in QueryService.query(
        #     '''
        #     select distinct ?clinical
        #     where {
        #         ?clinical a <http://research.tib.eu/p4-lucat/vocab/ClinicalTrial>.
        #     } limit 5
        #     '''
        #     , 'tib-lucat-kg'
        # )]
        # c_trials = [
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'),
        #     rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763')
        # ]
        c_trials = [
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/10600617'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/10612096'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/10803935'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/108201'),
            rdflib.URIRef('http://research.tib.eu/clarify2020/entity/1201891')
        ]
        for i, ct in enumerate(c_trials):
            c_trials[i] = MyOWLIndividual(ct, self)
        edges = []
        for p in patients:
            for ct in c_trials:
                sim = p.similarity(ct)
                edges.append((p, ct, sim))
                print(str(p), str(ct), sim)
        
        # patients = [
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self)
        # ]
        # c_trials = [
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self),
        #     MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self)
        # ]
        # edges = [
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self), 0.528174978956229),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self), 0.5831063612313613),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self), 0.6087629769921437),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self), 0.5758177008177008),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3426_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self), 0.5394903900112233),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self), 0.47638912270856715),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self), 0.5025575196408529),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self), 0.4996429573512907),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self), 0.48510580340441456),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3561_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self), 0.46193368873924434),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self), 0.4560727813852814),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self), 0.47188221500721506),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self), 0.45334190115440115),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self), 0.4557465277777778),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/3877_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self), 0.44074044011544017),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self), 0.41903911906677394),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self), 0.4119564002860463),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self), 0.41156588003933137),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self), 0.4170448958612676),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/10335_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self), 0.4132433293108072),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00049543'), self), 0.4658602327624067),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00057798'), self), 0.4195048309178744),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00130780'), self), 0.41722386912604303),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191126'), self), 0.44722222222222224),
        #     (MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/41994_LCPatient'), self), MyOWLIndividual(rdflib.URIRef('http://research.tib.eu/p4-lucat/entity/NCT00191763'), self), 0.43917709705753183)
        # ]
        utils.generate_bigraph(patients, c_trials, edges)
        # ---------generate graph files for semEP-----------

    def __get_all_properties(self):
        properties = set()
        # for s, p, o in self.o.triples((None, RDF.type, OWL.ObjectProperty)):
        #     if not isinstance(s, rdflib.BNode):
        #         properties.add(s)
        properties.update([rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?p a owl:ObjectProperty.
                filter(!isBlank(?p))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDF.type, OWL.DatatypeProperty)):
        #     if not isinstance(s, rdflib.BNode):
        #         properties.add(s)
        properties.update([rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?p a owl:DatatypeProperty.
                filter(!isBlank(?p))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDF.type, OWL.OntologyProperty)):
        #     if not isinstance(s, rdflib.BNode):
        #         properties.add(s)
        properties.update([rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?p a owl:OntologyProperty.
                filter(!isBlank(?p))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDF.type, OWL.AnnotationProperty)):
        #     if not isinstance(s, rdflib.BNode):
        #         properties.add(s)
        properties.update([rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?p a owl:AnnotationProperty.
                filter(!isBlank(?p))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, None, None)):
        #     if not isinstance(p, rdflib.BNode):
        #         properties.add(p)
        properties.update([rdflib.URIRef(v['p']) for v in QueryService.query(
            '''
            select distinct ?p
            where {
                ?s ?p ?o.
                filter(!isBlank(?p))
            }
            '''
            , self.endpoint
        )])
        properties = list(properties)

        return properties
    
    def __get_all_classes(self):
        classes = set()
        # for s, p, o in self.o.triples((None, RDF.type, OWL.Class)):
        #     if not isinstance(s, rdflib.BNode):
        #         classes.add(s)
        classes.update([rdflib.URIRef(v['c']) for v in QueryService.query(
            '''
            select distinct ?c
            where {
                ?c a owl:Class.
                filter(!isBlank(?c))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDF.type, RDFS.Class)):
        #     if not isinstance(s, rdflib.BNode):
        #         classes.add(s)
        classes.update([rdflib.URIRef(v['c']) for v in QueryService.query(
            '''
            select distinct ?c
            where {
                ?c a rdfs:Class.
                filter(!isBlank(?c))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDF.type, None)):
        #     if not isinstance(o, rdflib.BNode):
        #         classes.add(o)
        classes.update([rdflib.URIRef(v['o']) for v in QueryService.query(
            '''
            select distinct ?o
            where {
                ?s a ?o.
                filter(!isBlank(?o))
            }
            '''
            , self.endpoint
        )])
                
        # for s, p, o in self.o.triples((None, RDFS.subClassOf, None)):
        #     if s not in classes and not isinstance(s, rdflib.BNode):
        #         classes.add(s)
        #     if o not in classes and not isinstance(o, rdflib.BNode):
        #         classes.add(o)
        for v in QueryService.query(
            '''
            select distinct ?s ?o
            where {
                ?s rdfs:subClassOf ?o.
                filter(!isBlank(?o))
            }
            '''
            , self.endpoint
        ):
            classes.add(rdflib.URIRef(v['s']))
            classes.add(rdflib.URIRef(v['o']))
        
        # for s, p, o in self.o.triples((None, RDFS.domain, None)):
        #     if o not in classes and not isinstance(o, rdflib.BNode):
        #         classes.add(o)
        classes.update([rdflib.URIRef(v['o']) for v in QueryService.query(
            '''
            select distinct ?o
            where {
                ?s rdfs:domain ?o.
                filter(!isBlank(?o))
            }
            '''
            , self.endpoint
        )])
        # for s, p, o in self.o.triples((None, RDFS.range, None)):
        #     if o not in classes and not isinstance(o, rdflib.BNode):
        #         classes.add(o)
        classes.update([rdflib.URIRef(v['o']) for v in QueryService.query(
            '''
            select distinct ?o
            where {
                ?s rdfs:range ?o.
                filter(!isBlank(?o))
            }
            '''
            , self.endpoint
        )])
        classes = list(classes)
        
        return classes
    
    def __get_direct_superclasses(self, cls, exclude_bnodes=True):
        superclasses = set()
        # for s, p, o in self.o.triples((cls, RDFS.subClassOf, None)):
        #     if exclude_bnodes:
        #         if not isinstance(o, rdflib.BNode):
        #             superclasses.add(o)
        #     else:
        #         superclasses.add(o)
        if exclude_bnodes:
            superclasses.update([rdflib.URIRef(v['o']) for v in QueryService.query(
                '''
                select distinct ?o
                where {
                    ?s rdfs:subClassOf ?o.
                    filter(!isBlank(?o))
                }
                '''.replace('?s', cls.n3())
                , self.endpoint
            )])
        else:
            superclasses.update([rdflib.URIRef(v['o']) for v in QueryService.query(
                '''
                select distinct ?o
                where {
                    ?s rdfs:subClassOf ?o.
                }
                '''.replace('?s', cls.n3())
                , self.endpoint
            )])

        superclasses = list(superclasses)

        return superclasses
    
    def __get_all_superclasses(self, cls, superclasses=set(), exclude_bnodes=True):
        for scls in self.__get_direct_superclasses(cls, exclude_bnodes):
            superclasses.add(scls)
            self.__get_all_superclasses(scls, superclasses, exclude_bnodes)

        return superclasses
    
    def __get_direct_subclasses(self, cls, exclude_bnodes=True):
        subclasses = set()
        # for s, p, o in self.o.triples((None, RDFS.subClassOf, cls)):
        #     if exclude_bnodes:
        #         if not isinstance(s, rdflib.BNode):
        #             subclasses.add(s)
        #     else:
        #         subclasses.add(s)
        if exclude_bnodes:
            subclasses.update([rdflib.URIRef(v['s']) for v in QueryService.query(
                '''
                select distinct ?s
                where {
                    ?s rdfs:subClassOf ?o.
                    filter(!isBlank(?s))
                }
                '''.replace('?o', cls.n3())
                , self.endpoint
            )])
        else:
            subclasses.update([rdflib.URIRef(v['s']) for v in QueryService.query(
                '''
                select distinct ?s
                where {
                    ?s rdfs:subClassOf ?o.
                }
                '''.replace('?o', cls.n3())
                , self.endpoint
            )])
        subclasses = list(subclasses)
        
        return subclasses
    
    def __get_all_subclasses(self, cls, subclasses=[], exclude_bnodes=True):
        for scls in self.__get_direct_subclasses(cls, exclude_bnodes):
            subclasses.append(scls)
            self.__get_all_subclasses(scls, subclasses, exclude_bnodes)
        subclasses = list(set(subclasses))

        return subclasses
    
    def __get_all_class_siblings(self, cls, exclude_bnodes=True):
        siblings = set()
        for scls in self.__get_direct_superclasses(cls, exclude_bnodes):
            for child in self.__get_direct_subclasses(scls, exclude_bnodes):
                if child != cls:
                    siblings.add(child)
        siblings = list(siblings)

        return siblings
    
    def __get_top_classes(self):
        top_classes = set()
        for cls in self.__get_all_classes():
            scls = self.__get_direct_superclasses(cls)
            if not scls:
                top_classes.add(cls)
        top_classes = list(top_classes)

        return top_classes
    
    def __get_direct_super_properties(self, prop, exclude_bnode=True):
        super_properties = set()
        # for s, p, o in self.o.triples((prop, RDFS.subPropertyOf, None)):
        #     if exclude_bnode:
        #         if not isinstance(o, rdflib.BNode):
        #             super_properties.add(o)
        #     else:
        #         super_properties.add(o)
        if exclude_bnode:
            super_properties.update([rdflib.URIRef(v['o']) for v in QueryService.query(
                '''
                select distinct ?o
                where {
                    ?s rdfs:subPropertyOf ?o.
                    filter(!isBlank(?o))
                }
                '''.replace('?s', prop.n3())
                , self.endpoint
            )])
        else:
            super_properties.update([rdflib.URIRef(v['o']) for v in QueryService.query(
                '''
                select distinct ?o
                where {
                    ?s rdfs:subPropertyOf ?o.
                }
                '''.replace('?s', prop.n3())
                , self.endpoint
            )])
        super_properties = list(super_properties)

        return super_properties
    
    def __get_all_super_properties(self, prop, super_properties=[], exclude_bnode=True):
        for sprop in self.__get_direct_super_properties(prop, exclude_bnode):
            super_properties.append(sprop)
            self.__get_all_super_properties(sprop, super_properties, exclude_bnode)
        super_properties = list(set(super_properties))

        return super_properties

    def __get_direct_sub_properties(self, prop, exclude_bnode=True):
        sub_classes = set()
        # for s, p, o in self.o.triples((None, RDFS.subPropertyOf, prop)):
        #     if exclude_bnode:
        #         if not isinstance(s, rdflib.BNode):
        #             sub_classes.add(s)
        #     else:
        #         sub_classes.add(s)
        if exclude_bnode:
            sub_classes.update([rdflib.URIRef(v['s']) for v in QueryService.query(
                '''
                select distinct ?s
                where {
                    ?s rdfs:subPropertyOf ?o.
                    filter(!isBlank(?s))
                }
                '''.replace('?o', prop.n3())
                , self.endpoint
            )])
        else:
            sub_classes.update([rdflib.URIRef(v['s']) for v in QueryService.query(
                '''
                select distinct ?s
                where {
                    ?s rdfs:subPropertyOf ?o.
                }
                '''.replace('?o', prop.n3())
                , self.endpoint
            )])
        sub_classes = list(sub_classes)

        return sub_classes
    
    def __get_all_sub_properties(self, prop, sub_properties=[], exclude_bnode=True):
        for sprop in self.__get_direct_sub_properties(prop, exclude_bnode):
            sub_properties.append(sprop)
            self.__get_all_sub_properties(sprop, sub_properties, exclude_bnode)
        sub_properties = list(set(sub_properties))

        return sub_properties
    
    def __get_all_property_siblings(self, prop, exclude_bnode=True):
        siblings = set()
        for sprop in self.__get_direct_super_properties(prop, exclude_bnode):
            for child in self.__get_direct_sub_properties(sprop):
                if child != prop:
                    siblings.add(child)
        siblings = list(siblings)

        return siblings
      
    def __get_top_properties(self):
        top_properties = set()
        for prop in self.__get_all_properties():
            sprop = self.__get_direct_super_properties(prop)
            if not sprop:
                top_properties.add(prop)
        top_properties = list(top_properties)

        return top_properties
  
    def get_superobject_properties(self, x: rdflib.URIRef, direct: bool):
        super_prop = set()
        super_prop.add(OWL.topObjectProperty)  # same as Java version
        if direct:
            super_prop.update(self.__get_direct_super_properties(x))
            return super_prop
        li = self.__get_direct_super_properties(x)
        while len(li) > 0:
            step = self.__get_direct_super_properties(li[0])
            super_prop.add(li[0])
            del li[0]
            super_prop.update(step)
        super_prop = list(super_prop)
        
        return super_prop
    
    def get_super_classes(self, sub: rdflib.URIRef):
        anc = self.ancestors.get(sub)
        if anc is None:
            # anc = self.__get_all_superclasses(sub, [])
            anc = self.__get_all_superclasses(sub, set())
            # anc.append(OWL.Thing)
            anc.add(OWL.Thing)
            self.ancestors[sub] = anc
        # anc = list(set(anc))
        anc = list(anc)
        
        return anc

    def get_property_chains(self) -> list:
        property_chains = {}
        for s, _, o in self.o.triples((None, RDFS.subPropertyOf, None)):
            properties = [o for _, _, o in self.o.triples((s, OWL.propertyChain, None))]
            if len(properties) > 0:
                op = o
                r = self.get_OWL_relation(op.n3()[1:-1])

                relation_chain = []
                for o_chain in properties:
                    relation_chain.append(self.get_OWL_relation(o_chain.n3()[1:-1]))
                relation_chains = property_chains.get(r)
                if relation_chains is None:
                    relation_chains = []
                    property_chains[r] = relation_chains
                relation_chains.append(relation_chain)
        # for v in QueryService.query(
        #     '''
        #     select distinct ?s ?o
        #     where {
        #         ?s rdfs:subPropertyOf ?o.
        #     }
        #     '''
        #     , 'tib-lucat'
        # ):
        #     properties = [rdflib.URIRef(w['o']) for w in QueryService.query(
        #         '''
        #         select distinct ?o
        #         where {
        #             ?s owl:propertyChain ?o.
        #         }
        #         '''.replace('?s', '<' + v['s'] + '>')
        #         , 'tib-lucat'
        #     )]
        #     if len(properties) > 0:
        #         op = rdflib.URIRef(v['o'])
        #         r = self.get_OWL_relation(op.n3()[1:-1])

        #         relation_chain = []
        #         for o_chain in properties:
        #             relation_chain.append(self.get_OWL_relation(o_chain.n3()[1:-1]))
        #         relation_chains = property_chains.get(r)
        #         if relation_chains is None:
        #             relation_chains = []
        #             property_chains[r] = relation_chains
        #         relation_chains.append(relation_chain)
        
        return property_chains
    
    def get_relation_OWL_link(self, rel: OWLRelation) -> list:
        owl_links = set()

        for v in QueryService.query(
            '''
            select distinct ?p ?o
            where {
                ?s ?p ?o.
                filter(!isBlank(?o) && !isLiteral(?o))
            }'''.replace('?s', rel.get_OWL_object_property().n3())
            , self.endpoint
        ):
            if len(QueryService.query(
                    '''
                    select distinct ?o
                    where {
                        ?s a ?o.
                        filter(!isBlank(?o))
                    } limit 1
                    '''.replace('?s', '<' + v['o'] + '>')
                    , self.endpoint
                )) > 0:
                    link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_individual(v['o']))
                    owl_links.add(link)
            elif len(QueryService.query(
                    '''
                    select distinct ?s
                    where {
                        ?s a ?o.
                        filter(!isBlank(?s))
                    } limit 1
                    '''.replace('?o', '<' + v['o'] + '>')
                    , self.endpoint
                )) > 0:
                    link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_concept(v['o']))
                    owl_links.add(link)
        
        owl_links = list(owl_links)

        return owl_links
    
    def get_individual_OWL_link(self, ind: MyOWLIndividual) -> list:
        owl_links = set()
        # same_ind = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), OWL.sameAs, None))]
        # same_ind = [rdflib.URIRef(v['o']) for v in QueryService.query(
        #     '''
        #     select distinct ?o
        #     where {
        #         ?s owl:sameAs ?o.
        #     }
        #     '''.replace('?s', ind.get_OWL_named_individual().n3())
        #     , self.endpoint
        # )]
        for v in QueryService.query(
            '''
            select distinct ?p ?o
            where {
                ?s ?p ?o.
                filter(!isBlank(?o))
            }'''.replace('?s', ind.get_OWL_named_individual().n3())
            , self.endpoint
        ):
            if rdflib.URIRef(v['p']) in self._relations and \
                len(QueryService.query(
                    '''
                    select distinct ?o
                    where {
                        ?s a ?o.
                        filter(!isBlank(?o))
                    } limit 1
                    '''.replace('?s', '<' + v['o'] + '>')
                    , self.endpoint
                )) > 0:
                    if v['p'] not in ['http://research.tib.eu/clarify2020/vocab/hasFamilyHistory', 'http://research.tib.eu/clarify2020/vocab/hasDiagnosis', 'http://research.tib.eu/clarify2020/vocab/hasBio']:
                        continue
                    link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_individual(v['o']))
                    owl_links.add(link)
            elif rdflib.URIRef(v['p']) in self._relations and \
                len(QueryService.query(
                    '''
                    select distinct ?s
                    where {
                        ?s a ?o.
                        filter(!isBlank(?s))
                    } limit 1
                    '''.replace('?o', '<' + v['o'] + '>')
                    , self.endpoint
                )) > 0:
                    if v['p'] not in ['http://research.tib.eu/clarify2020/vocab/hasDiagnosis', 'http://research.tib.eu/clarify2020/vocab/hasBio']:
                        continue
                    link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_concept(v['o']))
                    owl_links.add(link)

            # if rdflib.URIRef(v['p']) in self._relations and rdflib.URIRef(v['o']) in self._individuals:
            #     link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_individual(v['o']))
            #     owl_links.add(link)
            # elif rdflib.URIRef(v['p']) in self._relations and rdflib.URIRef(v['o']) in self._concepts:
            #     link = OWLLink(self.get_OWL_relation(v['p']), self.get_OWL_concept(v['o']))
            #     owl_links.add(link)

        # for r in self.relations.values():
        #     neighs = set()
        #     p = r.get_OWL_object_property()

            # if r in self.property_chains.keys() \
            # or len([True for chain in self.property_chains.values() if r in chain]) > 0 \
            # or r in [s for s, _, _ in self.o.triples((None, RDF.type, OWL.TransitiveProperty))] \
            # or len(same_ind) > 0:
            #     neighs = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), p, None))]
            # else:
            #     set_aux = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), p, None))]
            #     for i in set_aux:
            #         if not isinstance(i, rdflib.BNode):
            #             neighs.add(i)
            # if r in self.property_chains.keys() \
            # or len([True for chain in self.property_chains.values() if r in chain]) > 0 \
            # or r in [rdflib.URIRef(v['s']) for v in QueryService.query(
            #     '''
            #     select distinct ?s
            #     where {
            #         ?s a owl:TransitiveProperty.
            #     }
            #     '''
            #     , 'tib-lucat'
            # )] \
            # or len(same_ind) > 0:
            #     neighs.update([rdflib.URIRef(v['o']) for v in QueryService.query(
            #         '''
            #         select distinct ?o
            #         where {
            #             ?s ?p ?o.
            #             filter(!isBlank(?o))
            #         }
            #         '''.replace('?s', ind.get_OWL_named_individual().n3()).replace('?p', p.n3())
            #         , 'tib-lucat'
            #     )])
            # else:
            #     neighs.update([rdflib.URIRef(v['o']) for v in QueryService.query(
            #         '''
            #         select distinct ?o
            #         where {
            #             ?s ?p ?o.
            #             filter(!isBlank(?o))
            #         }
            #         '''.replace('?s', ind.get_OWL_named_individual().n3()).replace('?p', p.n3())
            #         , 'tib-lucat'
            #     )])

            # for neigh in neighs:
            #     aux1 = self.individuals.get(neigh.n3()[1:-1])
            #     if aux1 is not None:
            #         link = OWLLink(r, aux1)
            #         owl_links.add(link)
            #     else:
            #         con = self.concepts.get(neigh.n3()[1:-1])
            #         if con is not None:
            #             link = OWLLink(r, aux1)
            #             owl_links.add(link)
        owl_links = list(owl_links)

        return owl_links
    
    def get_concept_OWL_link(self, c: OWLConcept) -> set:
        relations_exclude = ['www.w3.org']
        
        owl_links = set()
        outgoing_rel = [v['p'] for v in QueryService.query(
            '''
            SELECT DISTINCT ?p
            WHERE {
                ?p rdfs:domain ?sc.
                ?c rdfs:subClassOf* ?sc.
            }
            '''.replace('?c', c.cl.n3())
            , self.endpoint
        )]
        for out_rel in outgoing_rel:
            skip = False
            for re in relations_exclude:
                if re in out_rel:
                    skip = True
                    break
            if out_rel in self.relations.keys() and not skip:
                for v in QueryService.query(
                    '''
                    select distinct ?value
                    where {
                        ?s ?p ?ind.
                        ?ind a ?val.
                        ?val rdfs:subClassOf* ?value.
                    }
                    '''.replace('?p', '<' + out_rel + '>')
                    , self.endpoint
                ):
                    owl_links.add(OWLLink(self.get_OWL_relation(out_rel), self.get_OWL_concept(v['value'])))

        incident_rel = [v['p'] for v in QueryService.query(
            '''
            SELECT DISTINCT ?p
            WHERE {
                ?p rdfs:range ?sc.
                ?c rdfs:subClassOf* ?sc.
            }
            '''.replace('?c', c.cl.n3())
            , self.endpoint
        )]
        for in_rel in incident_rel:
            skip = False
            for re in relations_exclude:
                if re in out_rel:
                    skip = True
                    break
            if in_rel in self.relations.keys() and not skip:
                for v in QueryService.query(
                    '''
                    select distinct ?value
                    where {
                        ?ind ?p ?s.
                        ?ind a ?val.
                        ?val rdfs:subClassOf* ?value.
                    }
                    '''.replace('?p', '<' + in_rel + '>')
                    , self.endpoint
                ):
                    owl_links.add(OWLLink(self.get_OWL_relation(in_rel), self.get_OWL_concept(v['value'])))

        return owl_links
    
    def get_island(self, c: OWLConcept, visited: set=set()) -> set:
        island = set()
        # q = '''
        #     SELECT ?obj
        #     WHERE {
        #         ?c rdfs:subClassOf ?sc.
        #         ?sc owl:someValuesFrom ?obj.
        #     }  
        #     '''.replace('?c', c.cl.n3())

        # class_exps = [qres.obj for qres in self.o.query(q)]  # all super class expressions
        # for ce in class_exps:
        # for qres in self.o.query(q):
        
        for qres in self.o.query(self.prepared_queries['q1'], initBindings={'c': c.cl}):
            ce = qres.obj
            destiny_concept = self.get_OWL_concept(ce.n3()[1:-1])
            if destiny_concept not in visited:
                island.add(destiny_concept)
                visited.add(destiny_concept)
                island.update(self.get_island(destiny_concept, visited))
        # for qres in QueryService.query(self.prepared_queries['q1'].replace('?c', c.cl.n3()), 'tib-lucat'):
        #     ce = rdflib.URIRef(qres['obj'])
        #     destiny_concept = self.get_OWL_concept(ce.n3()[1:-1])
        #     if destiny_concept not in visited:
        #         island.add(destiny_concept)
        #         visited.add(destiny_concept)
        #         island.update(self.get_island(destiny_concept, visited))
        
        # print(list(self.prepared_queries['q1'].execute(['owl:' + c.cl.n3()[1:-10]])))
        # for qres in self.prepared_queries['q1'].execute([c.cl.n3()]):
        #     print(qres)
        #     ce = qres.obj
        #     destiny_concept = self.get_OWL_concept(ce.n3()[1:-1])
        #     if destiny_concept not in visited:
        #         island.add(destiny_concept)
        #         visited.add(destiny_concept)
        #         island.update(self.get_island(destiny_concept, visited))
        
        return island
    
    def check_OWL_link(self, c1: OWLConcept, r: OWLRelation, c2: OWLConcept) -> bool:
        a = c1.get_OWL_class()
        b = c2.get_OWL_class()
        p = r.get_OWL_object_property()
        # q = prepareQuery(
        #     '''
        #     SELECT ?c
        #     WHERE {
        #         ?c owl:onProperty ?p.
        #         ?c owl:someValuesFrom ?b.
        #     }
        #     '''
        # )

        # class_expr = self.o.query(self.prepared_queries['q2'], initBindings={'p': p, 'b': b})
        class_expr = QueryService.query(self.prepared_queries['q2'].replace('?p', p.n3()).replace('?b', b.n3()), 'tib-lucat-kg')

        cond1 = False
        for ce in class_expr:  #  loop class_expr wastes too much time !!!
            if (a, RDFS.subClassOf, ce.c) in self.o:
                cond1 = True
                break
        all_scs = self.__get_all_superclasses(a, superclasses=set(), exclude_bnodes=False)
        cond2 = False
        for ce in class_expr:
            if ce.c in all_scs:
                cond2 = True
                break

        if cond1 or cond2:
            return True
        else:
            return False
    
    def set_OWL_links(self, entities: list):
        concepts_E = set()
        for e in entities:
            if isinstance(e, OWLConcept):
                concepts_E.add(e)
            else:
                e.gei_neighbors()
        self.set_OWL_links_concepts(list(concepts_E))
    
    def set_OWL_links_concepts(self, concepts: list):
        axioms = {}
        
        for c in concepts:
            potential_neighbors = self.get_island(c)
            register = self.add_equivalent_axioms(c, potential_neighbors)
            axioms[c] = register
        
        for c in axioms.keys():
            a = c.get_OWL_class()
            register = axioms[c]
            links = set()
            for test in register.keys():
                expression = register[test]
                links.update(self.conformed_links(a, test, expression))
            c.set_neighbors(links)
        
        for c in axioms.keys():
            register = axioms[c]
            for test in register.keys():
                expression = register[test]
    
    def prof_LCS(self, set_x: list, set_y: list, x: rdflib.URIRef, y: rdflib.URIRef, typeofxy=None):
        if typeofxy is None:
            if x == y:
                return x
            
            common = [i for i in set_x if i in set_y]

            if len(common) == 0:
                return None
            
            lcs = common[0]

            maxProf = self.prof(lcs)
            for aux in common:
                if self.prof(aux) > maxProf:
                    maxProf = self.prof(aux)
                    lcs = aux
        else:
            if typeofxy == 'OWLConcept':
                if x == y:
                    return x
            
                common = [i for i in set_x if i in set_y]

                if len(common) == 0:
                    return None
                
                lcs = common[0]

                maxProf = self.prof(self.get_OWL_concept(lcs))
                for aux in common:
                    if self.prof(self.get_OWL_concept(aux)) > maxProf:
                        maxProf = self.prof(self.get_OWL_concept(aux))
                        lcs = aux
            elif typeofxy == 'OWLRelation':
                if x == y:
                    return x
            
                common = [i for i in set_x if i in set_y]

                if len(common) == 0:
                    return None
                
                lcs = common[0]

                maxProf = self.prof(self.get_OWL_relation(lcs))
                for aux in common:
                    if self.prof(self.get_OWL_concept(aux)) > maxProf:
                        maxProf = self.prof(self.get_OWL_relation(aux))
                        lcs = aux
            elif typeofxy == 'MyOWLIndividual':
                if x == y:
                    return x
            
                common = [i for i in set_x if i in set_y]

                if len(common) == 0:
                    return None
                
                lcs = common[0]

                maxProf = self.prof(self.get_OWL_individual(lcs))
                for aux in common:
                    if self.prof(self.get_OWL_individual(aux)) > maxProf:
                        maxProf = self.prof(self.get_OWL_individual(aux))
                        lcs = aux
        
        return lcs
    
    def dist(self, c1, c2):
        depth = 0
        if isinstance(c1, rdflib.URIRef):
            if c1 in self._concepts:
                dis = self.get_distance(c1, c2)
                if dis != -1:
                    return dis
                c = []  # Set of OWLClassExpression
                c.append(c1)
                while c2 not in c and len(c) > 0:
                    super_classes = []  # Set of OWLClassExpression
                    for i in c:
                        if i.n3():
                            super_classes.extend(self.__get_direct_superclasses(i))
                    c = super_classes
                    depth += 1
                self.set_distance(c1, c2, depth)
            elif c1 in self._relations:
                c = []
                c.append(c1)
                while c2 not in c and len(c) > 0:
                    superobject_properties = []
                    for i in c:
                        if i.n3():
                            superobject_properties.extend(self.__get_direct_super_properties(i))
                    c = superobject_properties
                    depth += 1
            elif c1 in self._individuals:
                c = []
                aux_set = self.get_types(c1, True)
                c.extend(aux_set)
                while c2 not in c and len(c) > 0:
                    super_classes = []
                    for i in c:
                        if i.n3():
                            super_classes.extend(self.__get_direct_superclasses(i))
                    c = super_classes
                    depth += 1
        else:
            if isinstance(c1, OWLConcept):
                c1 = c1.get_OWL_class()
                c2 = c2.get_OWL_class()
                dis = self.get_distance(c1, c2)
                if dis != -1:
                    return dis
                c = []  # Set of OWLClassExpression
                c.append(c1)
                while c2 not in c and len(c) > 0:
                    super_classes = []  # Set of OWLClassExpression
                    for i in c:
                        if i.n3():
                            super_classes.extend(self.__get_direct_superclasses(i))
                    c = super_classes
                    depth += 1
                self.set_distance(c1, c2, depth)
            elif isinstance(c1, OWLRelation):
                c1 = c1.get_OWL_object_property()
                c2 = c2.get_OWL_object_property()
                c = []
                c.append(c1)
                while c2 not in c and len(c) > 0:
                    superobject_properties = []
                    for i in c:
                        if i.n3():
                            superobject_properties.extend(self.__get_direct_super_properties(i))
                    c = superobject_properties
                    depth += 1
            elif isinstance(c1, MyOWLIndividual):
                c1 = c1.get_OWL_named_individual()
                c2 = c2.get_OWL_named_individual()
                c = []
                aux_set = self.get_types(c1, True)
                c.extend(aux_set)
                while c2 not in c and len(c) > 0:
                    super_classes = []
                    for i in c:
                        if i.n3():
                            super_classes.extend(self.__get_direct_superclasses(i))
                    c = super_classes
                    depth += 1

        
        return depth

    def dist_class(self, c1: rdflib.URIRef, c2: rdflib.URIRef):
        depth = 0
        dis = self.get_distance(c1, c2)
        if dis != -1:
            return dis
        c = []  # Set of OWLClassExpression
        c.append(c1)
        while c2 not in c and len(c) > 0:
            super_classes = []  # Set of OWLClassExpression
            for i in c:
                if i.n3():
                    super_classes.extend(self.__get_direct_superclasses(i))
            c = super_classes
            depth += 1
        self.set_distance(c1, c2, depth)

        return depth

    def dist_property(self, c1: rdflib.URIRef, c2: rdflib.URIRef):
        depth = 0
        c = []
        c.append(c1)
        while c2 not in c and len(c) > 0:
            superobject_properties = []
            for i in c:
                if i.n3():
                    superobject_properties.extend(self.__get_direct_super_properties(i))
            c = superobject_properties
            depth += 1
        
        return depth
    
    def dist_individual(self, c1: rdflib.URIRef, c2: rdflib.URIRef):
        depth = 0
        c = []
        aux_set = self.get_types(c1, True)
        c.extend(aux_set)
        while c2 not in c and len(c) > 0:
            super_classes = []
            for i in c:
                if i.n3():
                    super_classes.extend(self.__get_direct_superclasses(i))
            c = super_classes
            depth += 1
        
        return depth
    
    def prof(self, _class):
        depth = 0
        if isinstance(_class, rdflib.URIRef):
            if _class in self._concepts:
                if self.concept_profs.get(_class) is not None:
                    return self.concept_profs[_class]
                depth = self.dist_class(_class, OWL.Thing)
                if self.storing:
                    self.concept_profs[_class] = depth
            elif _class in self._relations:
                if self.relation_profs.get(_class) is not None:
                    return self.relation_profs[_class]
                depth = self.dist_property(_class, OWL.topObjectProperty)
                self.relation_profs[_class] = depth
        else:
            if isinstance(_class, OWLConcept):
                _class = _class.get_OWL_class()
                if self.concept_profs.get(_class) is not None:
                    return self.concept_profs[_class]
                depth = self.dist(self.get_OWL_concept(_class), self.get_OWL_concept(OWL.Thing))
                if self.storing:
                    self.concept_profs[_class] = depth
            elif isinstance(_class, OWLRelation):
                _class = _class.get_OWL_object_property()
                if self.relation_profs.get(_class) is not None:
                    return self.relation_profs[_class]
                depth = self.dist(self.get_OWL_relation(_class), self.get_OWL_relation(OWL.topObjectProperty))
                self.relation_profs[_class] = depth
        
        return depth
    
    def prof_class(self, _class):
        depth = 0
        if isinstance(_class, rdflib.URIRef):
            if self.concept_profs.get(_class) is not None:
                return self.concept_profs[_class]
            depth = self.dist_class(_class, OWL.Thing)
            if self.storing:
                self.concept_profs[_class] = depth
        elif isinstance(_class, OWLConcept):
            _class = _class.get_OWL_class()
            if self.concept_profs.get(_class) is not None:
                return self.concept_profs[_class]
            depth = self.dist(self.get_OWL_concept(_class), self.get_OWL_concept(OWL.Thing))
            if self.storing:
                self.concept_profs[_class] = depth
        
        return depth
    
    def prof_property(self, _class):
        depth = 0
        if isinstance(_class, rdflib.URIRef):
            if self.relation_profs.get(_class) is not None:
                return self.relation_profs[_class]
            depth = self.dist_property(_class, OWL.topObjectProperty)
            self.relation_profs[_class] = depth
        elif isinstance(_class, OWLRelation):
            _class = _class.get_OWL_object_property()
            if self.relation_profs.get(_class) is not None:
                return self.relation_profs[_class]
            depth = self.dist(self.get_OWL_relation(_class), self.get_OWL_relation(OWL.topObjectProperty))
            self.relation_profs[_class] = depth
        
        return depth

    def taxonomic_property_similarity(self, x, y):
        if x == y:
            return 1.0

        set_x = self.get_superobject_properties(x, False)
        set_x.append(x)
        set_y = self.get_superobject_properties(y, False)
        set_y.append(y)

        lcs = self.prof_LCS(set_x, set_y, x, y, typeofxy='OWLRelation')
        profLCS = self.prof_property(lcs)

        dxa = self.dist_property(x, lcs)
        dxroot = profLCS + dxa
        dya = self.dist_property(y, lcs)
        dyroot = profLCS + dya
        dtax = (dxa + dya) / (dxroot + dyroot)
        
        return 1-dtax
    
    def set_distance(self, c1, c2, d):
        aux = self.concept_distances.get(c1)
        if aux is None:
            aux = {}
            if self.storing:
                self.concept_distances[c1] = aux
        aux[c2] = d
    
    def get_distance(self, c1, c2):
        aux = self.concept_distances.get(c1)
        if aux is None:
            return -1
        else:
            d = aux.get(c2)
            if d is None:
                return -1
            else:
                return d
    
    def get_types(self, ind: rdflib.URIRef, direct: bool):
        classes = set()
        # clses = [o for _, _, o in self.o.triples((ind, RDF.type, None))]
        clses = [rdflib.URIRef(v['o']) for v in QueryService.query(
            '''
            select distinct ?o
            where {
                ?s a ?o.
                filter(!isBlank(?o))
            }
            '''.replace('?s', ind.n3())
            , self.endpoint
        )]
        if direct:
            classes.update(clses)
        else:
            for cls in clses:
                classes.update(self.__get_direct_superclasses(cls))
            classes.update(clses)
        classes = list(classes)
        
        return classes
    
    def get_OWL_relation(self, uri: str) -> OWLRelation:
        rel =  self.relations.get(uri)
        if rel is None:
            rel = OWLRelation(URIRef(uri), self)
            # self.relations[uri] = rel
        
        return rel
    
    def get_OWL_concept(self, uri: str) -> OWLConcept:
        con = self.concepts.get(uri)
        if con is None:
            con = OWLConcept(URIRef(uri), self)
            self.concepts[uri] = con
        
        return con
    
    def get_OWL_individual(self, uri: str) -> MyOWLIndividual:
        ind = self.individuals.get(uri)
        if ind is None:
            ind = MyOWLIndividual(URIRef(uri), self)
            self.individuals[uri] = ind
        
        return ind
    
    def get_my_OWL_logical_entity(self, uri: str) -> MyOWLLogicalEntity:
        con = self.concepts.get(uri)
        if con is None:
            con = self.individuals.get(uri)
        
        return con
    
    def get_ontology_prefix(self) -> str:
        return self.prefix
    
    def get_DCA(self, a, b):
        pass

    def get_LCS(self, a: OWLConcept, b: OWLConcept) -> OWLConcept:
        # comparison = AnnotationComparison(a, b)
        # lcs_concept = self.lcas.get(comparison)
        lcs_concept = None
        if lcs_concept is None:
            x, y = a.get_OWL_class(), b.get_OWL_class()
            set_x = self.get_super_classes(x)
            set_x.append(x)
            set_y = self.get_super_classes(y)
            set_y.append(y)
            lcs = self.prof_LCS(set_x, set_y, x, y, typeofxy='OWLConcept')
            lcs_concept = self.get_OWL_concept(lcs.n3()[1:-1])
            # if self.storing:
            #     self.lcas[comparison] = lcs_concept
        
        return lcs_concept

    def dps(self, x: OWLConcept, y: OWLConcept):
        lcs = self.get_LCS(x, y)

        prof_LCS = self.prof(lcs)
        dxa = self.dist(x, lcs)
        dya = self.dist(y, lcs)
        if prof_LCS + dxa + dya != 0:
            dps = 1.0 - prof_LCS / (prof_LCS + dxa + dya)
        else:
            dps = 1.0

        return 1.0 - dps

    def taxonomic_class_similarity(self, x: OWLConcept, y: OWLConcept):
        # dtax = self.dtax(x, y)
        dps = self.dps(x, y)

        return dps

    def taxonomic_individual_similarity(self, x: rdflib.URIRef, y: rdflib.URIRef):
        set_x = []
        set_y = []

        set_x = self.get_types(x, False)
        set_y = self.get_types(y, False)
        if len(set_x) == 0 or len(set_y) == 0:
            print('ERROR: ', x, ' or ', y, ' have no types.')
            return 0.0
        lcs = self.prof_LCS(set_x, set_y, set_x[0], None, typeofxy='OWLConcept')
        
        # if x in self._concepts and y in self._concepts:
        #     set_x = self.get_super_classes(x)
        #     set_x.append(x)
        #     set_x = list(set(x))
        #     set_y = self.get_super_classes(y)
        #     set_y.append(y)
        #     set_y = list(set(y))
        #     lcs = self.prof_LCS(set_x, set_y, x, y)
        
        if lcs is None:
            return 0.0
        
        prof_LCS = self.prof_class(lcs)
        # dxa = self.dist(x, lcs)
        dxa = self.dist_class(x, lcs)
        dxroot = prof_LCS + dxa
        # dya = self.dist(y, lcs)
        dya = self.dist_class(y, lcs)
        dyroot = prof_LCS + dya
        num = dxa + dya
        den = dxroot + dyroot
        dtax = num / den
        dtax = 1.0 - dtax

        return dtax


class AnnSim():
    def __init__(self, matrix=None):
        self.v1 = None
        self.v2 = None
        self.cost_matrix = None
        self.assignment = 0
        self.map_comparisons = matrix
    
    def matching(self, a: set, b: set, orig: OWLConcept, des: OWLConcept):
        if type(a[0]) != type(b[0]) and len(a) > 0 and len(b) > 0:
            print('Invalid comparison between ' + type(a[0]) + ' and ' + type(b[0]))
        else:
            if a == b:
                return 1.0
            if len(a) == 0 or len(b) == 0:
                return 0.0
            self.cost_matrix = np.zeros((len(a), len(b)))
            self.v1 = list(a)
            self.v2 = list(b)
            if self.map_comparisons is None:
                for i, s1 in enumerate(self.v1):
                    for j, s2 in enumerate(self.v2):
                        self.cost_matrix[i, j] = 1 - s1.similarity(s2, orig, des)
            else:
                for i, s1 in enumerate(self.v1):
                    for j, s2 in enumerate(self.v2):
                        comp = AnnotationComparison(s1, s2)
                        self.cost_matrix[i, j] = 1 - self.map_comparisons[comp]
            
            hungarn = HungarianAlgorithm(self.cost_matrix)
            self.assignment = hungarn.execute()

            sim = 0
            for i, aux in enumerate(self.assignment):
                if aux >= 0:
                    sim += 1 - self.cost_matrix[i, aux]
            
            return 2 * sim / (len(self.v1) + len(self.v2))
    
    def maximum_matching(self, a: set, b: set, orig: MyOWLLogicalEntity, des: MyOWLLogicalEntity):
        if type(a) != type(b) and len(a) == 0 and len(b) == 0:
            print('Invalid comparison between ' + type(a) + ' and ' + type(b))
        else:
            if a == b:
                return 1.0
            if len(a) == 0 or len(b) == 0:
                return 0.0
            self.cost_matrix = np.zeros((len(a), len(b)))
            self.v1 = list(a)
            self.v2 = list(b)
            if self.map_comparisons is None:
                for i, s1 in enumerate(self.v1):
                    for j, s2 in enumerate(self.v2):
                        self.cost_matrix[i, j] = s1.similarity(s2, orig, des)
            else:
                for i, s1 in enumerate(self.v1):
                    for j, s2 in enumerate(self.v2):
                        value = self.map_comparisons.get(AnnotationComparison(s1, s2))
                        if value is None:
                            value = 0.0
                        self.cost_matrix[i, j] = value
            
            sim = 0
            for i in range(len(self.v1)):
                max_row = 0
                for j in range(len(self.v2)):
                    if max_row < self.cost_matrix[i, j]:
                        max_row = self.cost_matrix[i, j]
                sim += max_row
            for j in range(len(self.v2)):
                max_col = 0
                for i in range(len(self.v1)):
                    if max_col < self.cost_matrix[i, j]:
                        max_col = self.cost_matrix[i, j]
                sim += max_col
            return sim / (len(a) + len(b))


class OWLRelation():
    def __init__(self, property: rdflib.URIRef, onto: MyOWLOntology):
        self.o = onto
        self.p = property
        self.neighbors = None
        self.uri = property.n3()[1:-1]
    
    def get_OWL_object_property(self) -> rdflib.URIRef:
        return self.p
    
    def __str__(self):
        return self.uri
    
    def taxonomic_similarity(self, r: OWLRelation):
        return self.o.taxonomic_property_similarity(self.get_OWL_object_property(), r.get_OWL_object_property())
    
    def similarity_neighbors(self, r: OWLRelation):
        if self.p == r.p:
            return 1.0
        
        bpm = BipartiteGraphMatching()

        if self.neighbors is None:
            self.neighbors = self.o.get_relation_OWL_link(self)
        if r.neighbors is None:
            r.neighbors = self.o.get_relation_OWL_link(r)
        
        try:
            restrict_neighbor_num = False
            if restrict_neighbor_num:
                neighbor_start, neighbor_end = 0, 100
                self.neighbors, r.neighbors = set(list(self.neighbors)[neighbor_start:neighbor_end]), set(list(r.neighbors)[neighbor_start:neighbor_end])
                sim = bpm.matching(self.neighbors, r.neighbors, self, r)
            else:
                sim = bpm.matching(self.neighbors, r.neighbors, self, r)
            return sim
        except Exception as e:
            e.with_traceback()
        
        return 0.0
    
    def similarity(self, r: OWLRelation):
        return (self.taxonomic_similarity(r) + self.similarity_neighbors(r)) / 2


class OWLLink():
    def __init__(self, r: OWLRelation, b: MyOWLLogicalEntity, exp: list=None):
        self.relation = r
        self.destiny = b
        self.explanations = exp
    
    def get_explanations(self):
        return self.explanations
    
    def __str__(self):
        return str(self.relation) + ' ' + str(self.destiny)
    
    def get_relation(self) -> OWLRelation:
        return self.relation
    
    def get_destiny(self) -> MyOWLLogicalEntity:
        return self.destiny
    
    def similarity(self, a: OWLLink, concept_a: MyOWLLogicalEntity, concept_b: MyOWLLogicalEntity):
        bmp = BipartiteGraphMatching()

        try:
            sim_tax_rel = self.relation.similarity(a.relation)
            sim_tax_des = self.destiny.taxonomic_similarity(a.destiny)
            # sim = (sim_tax_rel + sim_tax_des) / 2
            sim = 0.25 * sim_tax_rel + 0.75 * sim_tax_des
            return sim
        except Exception as e:
            e.with_traceback(sys.exc_info()[2])
        
        return 0.0


class MyOWLLogicalEntity(ABC):
    def __init__(self):
        self.uri = None
        self.o = None
        self.neighbors = None
    
    def set_neighbors(self, n):
        self.neighbors = n
    
    @abstractmethod
    def get_neighbors(self):
        pass

    def get_uri(self):
        return self.uri
    
    def __str__(self):
        return self.uri
    
    def get_name(self):
        pass

    def is_OWL_concept(self):
        return self.o.get_OWL_concept(self.uri) is not None

    def get_OWL_concept(self):
        return self.o.get_OWL_concept(self.uri)
    
    def is_my_OWL_individual(self):
        return self.o.get_OWL_individual(self.uri) is not None
    
    def get_my_OWL_individual(self):
        return self.o.get_OWL_individual(self.uri)

    @abstractmethod
    def get_OWL_logical_entity(self):
        pass

    @abstractmethod
    def taxonomic_similarity(self, c):
        pass

    @abstractmethod
    def similarity(self, a):
        pass

    @abstractmethod
    def similarity_neighbors(self, c):
        pass

    def IC_on_sim(self, c):
        inform_c = self.similarity_DCA(c)
        
        tax_sim = self.taxonomic_similarity(c)
        neigh_sim = 1
        if tax_sim > 0:
            neigh_sim = self.similarity_neighbors(c)
        
        return inform_c * tax_sim * neigh_sim
    
    def similarity_DCA(self, c):
        inform_c = 0
        dca = self.o.get_DCA(self, c)
        for con in dca:
            inform_c += con.get_IC()
        inform_c = inform_c / len(dca)

        return inform_c
    
    def similarity_IC(self, c):
        # inform_c = 0
        # lca = self.o.get_LCS(self, c)
        # ic = InformationContent.get_instance()
        # inform_c = ic.get_IC(lca)

        # return inform_c

        pass
    
    def on_sim(self, c):
        tax_sim = self.taxonomic_similarity(c)
        neigh_sim = self.similarity_neighbors(c)
        # sim = tax_sim * neigh_sim
        sim = (tax_sim + neigh_sim) / 2

        return sim
    
    def achim(self, c):
        tax_sim = self.taxonomic_similarity(c)
        sim = tax_sim
        neigh_sim = 1
        if tax_sim > 0:
            neigh_sim = self.similarity_neighbors(c)
        sim = tax_sim + neigh_sim

        return sim
    
    def similarity(self, a, org, des):
        return self.similarity(a)
    
    @abstractmethod
    def get_IC(self):
        pass


class OWLConcept(MyOWLLogicalEntity):
    def __init__(self, a: rdflib.URIRef, onto: MyOWLOntology):
        self.o = onto
        self.uri = a.n3()[1:-1]
        self.neighbors = None
        self.cl = a
        # self.name = self.uri
        self.satisfiable = self.is_satisfiable()
    
    def get_OWL_class(self):
        return self.cl
    
    def set_neighbors(self, n):
        self.neighbors = n
    
    def get_neighbors(self):
        if self.neighbors is None:
            self.neighbors = self.o.get_concept_OWL_link(self)
        return self.neighbors
    
    def dispose(self):
        self.neighbors.clear()
    
    def get_sub_concepts(self):
        return self.o.get_sub_concepts(self)
    
    def get_super_concepts(self):
        return self.o.get_ancestors(self)
    
    def get_URI(self):
        return self.uri
    
    def __str__(self):
        return self.uri
    
    def get_name(self):
        pass

    def is_satisfiable(self):
        pass

    def similarity_neighbors(self, c):
        if isinstance(c, OWLConcept):
            bpm = AnnSim()
            if self.neighbors is None:
                self.neighbors = self.o.get_concept_OWL_link(self)
            if c.neighbors is None:
                c.neighbors = self.o.get_concept_OWL_link(c)
            try:
                print([str(n) for n in self.neighbors], [str(n) for n in c.neighbors])
                restrict_neighbor_num = False
                if restrict_neighbor_num:
                    neighbor_start, neighbor_end = 0, 15
                    self.neighbors, c.neighbors = set(list(self.neighbors)[neighbor_start:neighbor_end]), set(list(c.neighbors)[neighbor_start:neighbor_end])
                    sim = bpm.maximum_matching(self.neighbors, c.neighbors, self, c)
                else:
                    sim = bpm.maximum_matching(self.neighbors, c.neighbors, self, c)
                return sim
            except Exception as e:
                e.with_traceback(sys.exc_info()[2])
            return 0.0
        if isinstance(c, MyOWLIndividual):
            bpm = AnnSim()
            if self.neighbors is None:
                self.neighbors = self.o.get_concept_OWL_link(self)
            if c.neighbors is None:
                c.neighbors = self.o.get_individual_OWL_link(c)
            try:
                sim = bpm.maximum_matching(self.neighbors, c.neighbors, self, c)
                return sim
            except Exception as e:
                e.with_traceback()
            return 0.0
        print('Invalid comparison between ' + self + ' and ' + c)
    
    def similarity_neighbors_achim(self, c, exp_radius):
        na = []
        nb = []
        ta = {self: 1}
        tb = {c: 1}
        na.append(ta)
        nb.append(tb)

        for i in range(1, exp_radius + 1):
            aux_a = na[i - 1]
            aux_a_next = {}
            aux_b = nb[i - 1]
            aux_b_next = {}

            for con in aux_a.keys():
                la = self.o.get_direct_neighbors(con)
                for l in la:
                    neigh = l.get_destiny()
                    n = aux_a_next.get(l.get_destiny())
                    if n is None:
                        n = 0
                    aux_a_next[neigh] = n + 1
            na.append(aux_a_next)

            for con in aux_b.keys():
                la = self.o.get_direct_neighbors(con)
                for l in la:
                    neigh = l.get_destiny()
                    n = aux_b_next.get(l.get_destiny())
                    if n is None:
                        n = 0
                    aux_b_next[neigh] = n + 1
            nb.append(aux_b_next)
        
        ela = []
        intersection = []
        for m in na:
            ela.extend(m.keys())
        intersection.extend(ela)
        elb = []
        for m in nb:
            elb.extend(m.keys())
        intersection = [x for x in intersection if x in elb]

        total = 0

        for con in intersection:
            sum_a, sum_b = 0, 0
            for i in range(1, exp_radius + 1):
                neighs_a = na[i]
                x = neighs_a.get(con)
                if x is None:
                    x = 0
                sum_a += np.power(0.5, i) * x
            for i in range(1, exp_radius + 1):
                neighs_b = nb[i]
                x = neighs_b.get(con)
                if x is None:
                    x = 0
                sum_b += np.power(0.5, i) * x
            total += sum_a * sum_b
        
        max_a, max_b = 0, 0
        for con in ela:
            sum_a = 0
            for i in range(1, exp_radius + 1):
                neighs_a = na[i]
                x = neighs_a.get(con)
                if x is None:
                    x = 0
                sum_a += np.power(0.5, i) * x
            max_a += sum_a ** 2
        for con in elb:
            sum_b = 0
            for i in range(1, exp_radius + 1):
                neighs_b = nb[i]
                x = neighs_b.get(con)
                if x is None:
                    x = 0
                sum_a += np.power(0.5, i) * x
            max_b += sum_b ** 2
        
        maxi = max(max_a, max_b)
        if total > maxi:
            print('ERROR: Total and max incorrectly computed')
        if maxi == 0:
            maxi = 1
        
        return total / maxi
    
    def taxonomic_similarity(self, c):
        if isinstance(c, OWLConcept):
            return self.o.taxonomic_class_similarity(self, c)
        elif isinstance(c, MyOWLIndividual):
            concepts = c.get_types()
            maxi = 0
            for cn in concepts:
                sim = self.o.taxonomic_class_similarity(self, cn)
                if sim > maxi:
                    maxi = sim
            return maxi
    
    def is_sub_concept_of(self, c: 'OWLConcept'):
        return self.o.is_subclass_of(self.get_OWL_class(), c.get_OWL_class())
    
    def get_IC(self):
        pass

    def similarity_IC(self, c):
        pass

    def similarity_MICA(self, c):
        pass

    def get_depth(self):
        self.get_OWL_class()
        return self.o.prof(self.cl)
    
    def similarity(self, c):
        if isinstance(c, OWLConcept):
            # if not self.satisfiable or not c.satisfiable:
            #     return 0
            if self == c:
                return 1.0
            sim = self.on_sim(c)
            return sim
        elif isinstance(c, MyOWLIndividual):
            c.similarity(self)
    
    def similarity_bypass(self, c):
        pass

    def get_LCA(self, b):
        return self.o.get_LCS(self, b)
    
    def get_OWL_logical_entity(self):
        return self.cl


class MyOWLIndividual(MyOWLLogicalEntity):
    def __init__(self, a: rdflib.URIRef, onto: MyOWLOntology):
        self.o = onto
        self.uri = a.n3()[1:-1]
        self.neighbors = None
        self.ind = a
    
    def get_OWL_named_individual(self) -> rdflib.URIRef:
        if self.ind is None:
            self.ind = self.o.get_OWL_individual(self.uri)
        
        return self.ind
    
    def get_neighbors(self):
        if self.neighbors is None:
            self.neighbors = self.o.get_individual_OWL_link(self)
        
        return self.neighbors
    
    def get_IC(self):
        pass
    
    def get_OWL_logical_entity(self):
        pass
    
    def similarity_neighbors(self, c: MyOWLIndividual):
        bpm = BipartiteGraphMatching()
        
        if self.neighbors is None:
            self.neighbors = self.o.get_individual_OWL_link(self)
        if c.neighbors is None:
            c.neighbors = self.o.get_individual_OWL_link(c)
        
        try:
            restrict_neighbor_num = False
            if restrict_neighbor_num:
                neighbor_start, neighbor_end = 0, 1
                self.neighbors, c.neighbors = set(list(self.neighbors)[neighbor_start:neighbor_end]), set(list(c.neighbors)[neighbor_start:neighbor_end])
                sim = bpm.matching(self.neighbors, c.neighbors, self, c)
            else:
                sim = bpm.matching(self.neighbors, c.neighbors, self, c)
            return sim
        except Exception as e:
            e.with_traceback()
        
        return 0.0
    
    def get_types(self) -> list:
        concepts = []
        cls = self.o.get_types(self.ind, True)
        for cl in cls:
            con = self.o.get_OWL_concept(cl.n3()[1:-1])
            concepts.append(con)
        concepts = list(set(concepts))

        return concepts
    
    def taxonomic_similarity(self, c):
        if isinstance(c, MyOWLIndividual):
            return self.o.taxonomic_individual_similarity(self.get_OWL_named_individual(), c.get_OWL_named_individual())
        if isinstance(c, OWLConcept):
            cls = self.get_types()
            sim, maxi = 0, 0
            for con in cls:
                sim = c.taxonomic_similarity(con)
                if sim > maxi:
                    maxi = sim
            return maxi

    def similarity(self, c):
        if isinstance(c, MyOWLIndividual):
            if self == c:
                return 1.0
            sim = self.on_sim(c)
            return sim
        if isinstance(c, OWLConcept):
            tax_sim = self.taxonomic_similarity(c)
            neigh_sim = 1
            if tax_sim > 0:
                neigh_sim = self.similarity_neighbors(c)
            sim = tax_sim * neigh_sim
            return sim


class BipartiteGraphMatching():
    def __init__(self):
        self.cost_matrix = None
        self.assignment = None
    
    def matching(self, a: list, b: list, orig: MyOWLLogicalEntity, des: MyOWLLogicalEntity):
        if len(a) == 0 or len(b) == 0:
            return 0.0
        if set(a) == set(b):
            return 1.0
        
        self.cost_matrix = np.zeros((len(a), len(b)))
        for i, s1 in enumerate(a):
            for j, s2 in enumerate(b):
                self.cost_matrix[i, j] = 1 - s1.similarity(s2, orig, des)
        
        hungarn = HungarianAlgorithm(self.cost_matrix)
        self.assignment = hungarn.execute()

        sim = 0
        for i in range(len(self.assignment)):
            aux = self.assignment[i]
            if aux >= 0:
                sim += 1 - self.cost_matrix[i, aux]
        
        return 2*sim/(2*max(len(a), len(b)))


class HungarianAlgorithm():
    def __init__(self, cost_matrix: np.array):
        self.dim = max(cost_matrix.shape[0], cost_matrix.shape[1])
        self.rows = cost_matrix.shape[0]
        self.cols = cost_matrix.shape[1]
        self.cost_matrix = np.zeros((self.dim, self.dim))
        for w in range(self.dim):
            if w < cost_matrix.shape[0]:
                # self.cost_matrix[w] = cost_matrix[:len(cost_matrix)-1]
                self.cost_matrix[w, :cost_matrix.shape[1]] = cost_matrix[w]
            else:
                np.append(self.cost_matrix, np.zeros((1, self.dim)), axis=0)
        self.label_by_worker = np.zeros(self.dim)
        self.label_by_job = np.zeros(self.dim)
        self.min_slack_worker_by_job = np.zeros(self.dim, dtype=np.int32)
        self.min_slack_value_by_job = np.zeros(self.dim)
        self.committed_workers = np.zeros(self.dim, dtype=np.bool8)
        self.parent_worker_by_committed_job = np.zeros(self.dim, dtype=np.int32)
        self.match_job_by_worker = np.zeros(self.dim, dtype=np.int32)
        self.match_job_by_worker[:] = -1
        self.match_worker_by_job = np.zeros(self.dim, dtype=np.int32)
        self.match_worker_by_job[:] = -1
    
    def compute_initial_feasible_solution(self):
        for j in range(self.dim):
            self.label_by_job[j] = np.Infinity
        for w in range(self.dim):
            for j in range(self.dim):
                if self.cost_matrix[w, j] < self.label_by_job[j]:
                    self.label_by_job[j] = self.cost_matrix[w, j]
    
    def execute(self) -> np.array:
        self.reduce()
        self.compute_initial_feasible_solution()
        self.greedy_match()

        w = self.fetch_unmatched_worker()
        while w < self.dim:
            self.initialize_phase(w)
            self.execute_phase()
            w = self.fetch_unmatched_worker()
        result = self.match_job_by_worker[:self.rows]
        for w in range(len(result)):
            if result[w] >= self.cols:
                result[w] = -1
        
        return result
    
    def execute_phase(self):
        while True:
            min_slack_worker, min_slack_job = -1, -1
            min_slack_value = np.Infinity
            for j in range(self.dim):
                if self.parent_worker_by_committed_job[j] == -1:
                    if self.min_slack_value_by_job[j] < min_slack_value:
                        min_slack_value = self.min_slack_value_by_job[j]
                        min_slack_worker = self.min_slack_worker_by_job[j]
                        min_slack_job = j
            if min_slack_value > 0:
                self.update_labeling(min_slack_value)
            self.parent_worker_by_committed_job[min_slack_job] = min_slack_worker
            if self.match_worker_by_job[min_slack_job] == -1:
                committed_job = min_slack_job
                parent_worker = self.parent_worker_by_committed_job[committed_job]
                while True:
                    temp = self.match_job_by_worker[parent_worker]
                    self.match(parent_worker, committed_job)
                    committed_job = temp
                    if committed_job == -1:
                        break
                    parent_worker = self.parent_worker_by_committed_job[committed_job]
                return
            else:
                worker = self.match_worker_by_job[min_slack_job]
                self.committed_workers[worker] = True
                for j in range(self.dim):
                    if self.parent_worker_by_committed_job[j] == -1:
                        slack = self.cost_matrix[worker, j] - self.label_by_worker[worker] - self.label_by_job[j]
                        if self.min_slack_value_by_job[j] > slack:
                            self.min_slack_value_by_job[j] = slack
                            self.min_slack_worker_by_job[j] = worker
    
    def fetch_unmatched_worker(self):
        for w in range(self.dim):
            if self.match_job_by_worker[w] == -1:
                break
            w += 1
        
        return w
    
    def greedy_match(self):
        for w in range(self.dim):
            for j in range(self.dim):
                if self.match_job_by_worker[w] == -1 and self.match_worker_by_job[j] == -1 \
                    and self.cost_matrix[w, j] - self.label_by_worker[w] - self.label_by_job[j] == 0:
                    self.match(w, j)
    
    def initialize_phase(self, w: np.int32):
        self.committed_workers[:] = False
        self.parent_worker_by_committed_job[:] = -1
        self.committed_workers[w] = True
        for j in range(self.dim):
            self.min_slack_value_by_job[j] = self.cost_matrix[w, j] - self.label_by_worker[w] \
                                                - self.label_by_job[j]
            self.min_slack_worker_by_job[j] = w
    
    def match(self, w: np.int32, j: np.int32):
        self.match_job_by_worker[w] = j
        self.match_worker_by_job[j] = w
    
    def reduce(self):
        for w in range(self.dim):
            mi = np.Infinity
            for j in range(self.dim):
                if self.cost_matrix[w, j] < mi:
                    mi = self.cost_matrix[w, j]
            for j in range(self.dim):
                self.cost_matrix[w, j] -= mi
        mi = np.zeros(self.dim)
        for j in range(self.dim):
            mi[j] = np.Infinity
        for w in range(self.dim):
            for j in range(self.dim):
                if self.cost_matrix[w, j] < mi[j]:
                    mi[j] = self.cost_matrix[w, j]
        for w in range(self.dim):
            for j in range(self.dim):
                self.cost_matrix[w, j] -= mi[j]
    
    def update_labeling(self, slack):
        for w in range(self.dim):
            if self.committed_workers[w]:
                self.label_by_worker[w] += slack
        for j in range(self.dim):
            if self.parent_worker_by_committed_job[j] != -1:
                self.label_by_job[j] -= slack
            else:
                self.min_slack_value_by_job[j] -= slack


class AnnotationComparison():
    def __init__(self, a, b):
        self.concept_A = a
        self.concept_B = b
        if str(self.concept_A) < str(self.concept_B):
            self.hash = hash(self.concept_A) ^ hash(self.concept_B)
        else:
            self.hash = hash(self.concept_B) ^ hash(self.concept_A)
    
    def __hash__(self) -> int:
        return self.hash
    
    def __str__(self) -> str:
        return str(self.concept_A) + '\t' + str(self.concept_B)
    
    def __eq__(self, o: object) -> bool:
        if isinstance(o, AnnotationComparison):
            return self.concept_A == o.concept_A \
                and  self.concept_B == o.concept_B \
                    or self.concept_A == o.concept_B \
                        and self.concept_B == o.concept_A
        return False
    
    def get_concept_A(self):
        return self.concept_A
    
    def get_concept_B(self):
        return self.concept_B


class QueryService():
    # To add support to more endpoints, add here:
    endpoints = {
        'tib-wikidata': 'http://node3.research.tib.eu:4010/sparql',
        'tib-wikidata-2': 'http://node3.research.tib.eu:4012/sparql',
        'wikidata': 'https://query.wikidata.org/sparql',
        'dbpedia': 'https://dbpedia.org/sparql',
        'tib-dbpedia': 'http://node3.research.tib.eu:4002/sparql',
        'tib-lucat': 'https://labs.tib.eu/sdm/p4-lucat-v2/sparql',
        'tib-lucat-onto': 'https://labs.tib.eu/sdm/p4lucat_mappings_kg/sparql',
        'tib-lucat-kg': 'https://labs.tib.eu/sdm/p4lucat_kg/sparql',
        'tib-node2': 'http://node2.research.tib.eu:41111/sparql',
        'clarify': 'https://labs.tib.eu/sdm/clarify_kg/sparql'
    }

    @classmethod
    def query(cls, q=None, source=None):
        if q is None:
            print('Error! No query is given.')
            return None
        if source is None:
            print('Error! No endpoint is given.')
            return None
        
        endpoint = cls.endpoints[source]

        s = requests.Session()
        headers = {
            'Accept': 'application/json'
        }
        data = {'query': q}
        s.headers.update(headers)

        response = s.post(endpoint, data=data, headers=headers)

        if response.status_code != 200:
            print('Query to endpoint %s, returned code %s' % (endpoint, response.status_code))
            print(response.text)
        
        content = json.loads(response.text)
        
        if 'results' not in content or 'bindings' not in content['results']:
            print('The query result is empty.')
            return None
        
        results = content['results']['bindings']

        query_results = []
        for result in results:
            triple = {}
            for key in result.keys():
                triple[key] = result[key]['value']
            query_results.append(triple)
        
        return query_results


if __name__ == '__main__':
    # MyOWLOntology('ontologies/go.owl')
    # MyOWLOntology('ontologies/go-plus.owl')
    # MyOWLOntology('ontologies/owl.owl')
    # MyOWLOntology('ontologies/OntologyID(Anonymous-720361).owl')
    # MyOWLOntology('ontologies/mls.owl')
    # MyOWLOntology('ontologies/DBPedia_People_2016-07-22_19-50-12.nq')
    MyOWLOntology('ontologies/patients.nt')
    # MyOWLOntology('ontologies/p4-lucat_v3.owl')
    # MyOWLOntology(['ontologies/p4-lucat_v3.owl', 'ontologies/patients.nt'])