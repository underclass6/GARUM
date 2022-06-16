from __future__ import annotations
from typing import Type
from abc import ABC, abstractmethod
import numpy as np
import rdflib
from rdflib import Graph, URIRef
from rdflib import RDF, RDFS
from rdflib import OWL

class MyOWLOntology():
    def __init__(self, ont_file, reasoner_name):
        self.concepts = {}  # string: owl_concept
        self.individuals = {}  # string: myowl_individual
        self.relations = {}  # string: wol_relation
        self.ancestors = {}  # owl_logical_entity: set of owl_class
        self.concept_distances = {}  # owl_class: dictionary of owl_class: integer
        self.lcas = {}  # annotation_comparison: owl_concept
        self.concept_profs = {}
        self.relation_profs = {}
        self.property_chains = {}
        self.exp_id = 0
        self.storing = True

        self.o = Graph()
        print('Parsing ontology...')
        self.o.parse(ont_file)
        print('Finished')

        object_properties = list(set([s for s, _, _ in self.o.triples((None, RDF.type, OWL.ObjectProperty))]))
        if OWL.topObjectProperty in object_properties:
            object_properties.remove(OWL.topObjectProperty)
        for op in object_properties:
            self.relations[op.n3()[1:-1]] = OWLRelation(op, self)
        print('Relations read')

        classes = self.__get_all_classes()
        classes.append(OWL.Thing)
        for cl in classes:
            self.concepts[cl.n3()[1:-1]] = OWLConcept(cl, self)
        print('Classes read')

        indivs = []
        for cl in classes:
            indivs.extend([s for s, _, _ in self.o.triples((None, RDF.type, cl)) if not isinstance(s, rdflib.BNode)])
        indivs = list(set(indivs))
        for ind in indivs:
            if self.concepts.get(ind.n3()[1:-1]) is None:
                self.individuals[ind.n3()[1:-1]] = MyOWLIndividual(ind, self)
        
        # [_ for _ in self.individuals.values()][0].get_neighbors()
        # self.individuals['http://purl.org/obo/owl/obo#gosubset_prok'].get_neighbors()
        # print(self.get_property_chains())
        # for ind in [_ for _ in self.individuals.values()][:10]:
        #     print(ind.get_neighbors())


    def __get_all_classes(self):
        classes = []
        for s, p, o in self.o.triples((None, RDF.type, OWL.Class)):
            if not isinstance(s, rdflib.BNode):
                classes.append(s)
        for s, p, o in self.o.triples((None, RDF.type, RDFS.Class)):
            if not isinstance(s, rdflib.BNode):
                classes.append(s)
        for s, p, o in self.o.triples((None, RDF.type, None)):
            if not isinstance(o, rdflib.BNode):
                classes.append(o)
                
        for s, p, o in self.o.triples((None, RDFS.subClassOf, None)):
            if s not in classes and not isinstance(s, rdflib.BNode):
                classes.append(s)
            if o not in classes and not isinstance(o, rdflib.BNode):
                classes.append(o)
        
        for s, p, o in self.o.triples((None, RDFS.domain, None)):
            if o not in classes and not isinstance(o, rdflib.BNode):
                classes.append(o)
        for s, p, o in self.o.triples((None, RDFS.range, None)):
            if o not in classes and not isinstance(o, rdflib.BNode):
                classes.append(o)
        classes = list(set(classes))
        
        return classes
    
    def __get_direct_superclasses(self, cls, exclude_bnodes=True):
        superclasses = []
        for s, p, o in self.o.triples((cls, RDFS.subClassOf, None)):
            if exclude_bnodes:
                if not isinstance(o, rdflib.BNode):
                    superclasses.append(o)
            else:
                superclasses.append(o)
        superclasses = list(set(superclasses))

        return superclasses
    
    def __get_all_superclasses(self, cls, superclasses=[], exclude_bnodes=True):
        for scls in self.__get_direct_superclasses(cls, exclude_bnodes):
            superclasses.append(scls)
            self.__get_all_superclasses(scls, superclasses, exclude_bnodes)
        superclasses = list(set(superclasses))

        return superclasses
    
    def __get_direct_subclasses(self, cls, exclude_bnodes=True):
        subclasses = []
        for s, p, o in self.o.triples((None, RDFS.subClassOf, cls)):
            if exclude_bnodes:
                if not isinstance(s, rdflib.BNode):
                    subclasses.append(s)
            else:
                subclasses.append(s)
        subclasses = list(set(subclasses))
        
        return subclasses
    
    def __get_all_subclasses(self, cls, subclasses=[], exclude_bnodes=True):
        for scls in self.__get_direct_subclasses(cls, exclude_bnodes):
            subclasses.append(scls)
            self.__get_all_subclasses(scls, subclasses, exclude_bnodes)
        subclasses = list(set(subclasses))

        return subclasses
    
    def __get_all_class_siblings(self, cls, exclude_bnodes=True):
        siblings = []
        for scls in self.__get_direct_superclasses(cls, exclude_bnodes):
            for child in self.__get_direct_subclasses(scls, exclude_bnodes):
                if child != cls:
                    siblings.append(child)
        siblings = list(set(siblings))

        return siblings
    
    def __get_top_classes(self):
        top_classes = []
        for cls in self.__get_all_classes():
            scls = self.__get_direct_superclasses(cls)
            if not scls:
                top_classes.append(cls)
        top_classes = list(set(top_classes))

        return top_classes
    
    def __get_all_properties(self):
        properties = []
        for s, p, o in self.o.triples((None, RDF.type, RDF.Property)):
            if not isinstance(s, rdflib.BNode):
                properties.append(s)
        for s, p, o in self.o.triples((None, RDF.type, OWL.ObjectProperty)):
            if not isinstance(s, rdflib.BNode):
                properties.append(s)
        for s, p, o in self.o.triples((None, RDF.type, OWL.DatatypeProperty)):
            if not isinstance(s, rdflib.BNode):
                properties.append(s)
        for s, p, o in self.o.triples((None, RDF.type, OWL.AnnotationProperty)):
            if not isinstance(s, rdflib.BNode):
                properties.append(s)
        properties = list(set(properties))

        return properties
    
    def __get_direct_super_properties(self, prop, exclude_bnode=True):
        super_properties = []
        for s, p, o in self.o.triples((prop, RDFS.subPropertyOf, None)):
            if exclude_bnode:
                if not isinstance(o, rdflib.BNode):
                    super_properties.append(o)
            else:
                super_properties.append(o)
        super_properties = list(set(super_properties))

        return super_properties
    
    def __get_all_super_properties(self, prop, super_properties=[], exclude_bnode=True):
        for sprop in self.__get_direct_super_properties(prop, exclude_bnode):
            super_properties.append(sprop)
            self.__get_all_super_properties(sprop, super_properties, exclude_bnode)
        super_properties = list(set(super_properties))

        return super_properties

    def __get_direct_sub_properties(self, prop, exclude_bnode=True):
        sub_classes = []
        for s, p, o in self.o.triples((None, RDFS.subPropertyOf, prop)):
            if exclude_bnode:
                if not isinstance(s, rdflib.BNode):
                    sub_classes.append(s)
            else:
                sub_classes.append(s)
        sub_classes = list(set(sub_classes))

        return sub_classes
    
    def __get_all_sub_properties(self, prop, sub_properties=[], exclude_bnode=True):
        for sprop in self.__get_direct_sub_properties(prop, exclude_bnode):
            sub_properties.append(sprop)
            self.__get_all_sub_properties(sprop, sub_properties, exclude_bnode)
        sub_properties = list(set(sub_properties))

        return sub_properties
    
    def __get_all_property_siblings(self, prop, exclude_bnode=True):
        siblings = []
        for sprop in self.__get_direct_super_properties(prop, exclude_bnode):
            for child in self.__get_direct_sub_properties(sprop):
                if child != prop:
                    siblings.append(child)
        siblings = list(set(siblings))

        return siblings
      
    def __get_top_properties(self):
        top_properties = []
        for prop in self.__get_all_properties():
            sprop = self.__get_direct_super_properties(prop)
            if not sprop:
                top_properties.append(prop)
        top_properties = list(set(top_properties))

        return top_properties
  
    def get_superobject_properties(self, x: rdflib.URIRef, direct: bool):
        super_prop = []
        # ...... add top object property to super_prop
        # top_properties = self.__get_top_properties()
        # print(top_properties)
        # object_properties = list(set([op for op, _, _ in self.o.triples((None, RDF.type, OWL.ObjectProperty))]))
        # for prop in top_properties:
        #     if prop in object_properties:
        #         super_prop.append(prop)
        super_prop.append(OWL.topObjectProperty)  # same as Java version
        if direct:
            super_prop.extend(self.__get_direct_super_properties(x))
            return super_prop
        li = self.__get_direct_super_properties(x)
        while len(li) > 0:
            step = self.__get_direct_super_properties(li[0])
            super_prop.append(li[0])
            del li[0]
            super_prop.extend(step)
        
        return super_prop
    
    def get_super_classes(self, sub: rdflib.URIRef):
        anc = self.ancestors.get(sub)
        if anc is None:
            anc = self.__get_all_superclasses(sub, [])
            anc.append(OWL.Thing)
            self.ancestors[sub] = anc
        
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
        
        return property_chains
    
    def get_individual_OWL_link(self, ind: MyOWLIndividual) -> list:
        owl_links = []
        same_ind = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), OWL.sameAs, None))]
        for r in self.relations.values():
            p = r.get_OWL_object_property()
            neighs = []

            if r in self.property_chains.keys() \
            or len([True for chain in self.property_chains.values() if r in chain]) > 0 \
            or r in [s for s, _, _ in self.o.triples((None, RDF.type, OWL.TransitiveProperty))] \
            or len(same_ind) > 0:
                neighs = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), p, None))]
            else:
                set_aux = [o for _, _, o in self.o.triples((ind.get_OWL_named_individual(), p, None))]
                for i in set_aux:
                    if not isinstance(i, rdflib.BNode):
                        neighs.append(i)
            
            print('Listo')
            for neigh in neighs:
                if not isinstance(neigh, rdflib.BNode):
                    aux1 = self.individuals.get(neigh.n3()[1:-1])
                    if aux1 is not None:
                        exps = []
                        link = OWLLink(r, aux1, exps)
                        owl_links.append(link)
                    else:
                        exps = []
                        con = self.concepts.get(neigh.n3()[1:-1])
                        if con is not None:
                            link = OWLLink(r, aux1, exps)
                            owl_links.append(link)
        
        return owl_links
    
    def prof_LCS(self, set_x: list, set_y: list, x: rdflib.URIRef, y: rdflib.URIRef):
        if x == y:
            return x
        
        common = [i for i in set_x if i in set_y]

        lcs = common[0]

        maxProf = self.prof(lcs)
        for aux in common:
            if self.prof(aux) > maxProf:
                maxProf = self.prof(aux)
                lcs = aux
        
        return lcs
    
    def dist(self, c1: rdflib.URIRef, c2: rdflib.URIRef):
        depth = 0
        if c1 in list(self.o.subjects(RDF.type, OWL.Class)):
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
        elif c1 in list(self.o.subjects(RDF.type, OWL.ObjectProperty)):
            c = []
            c.append(c1)
            while c2 not in c and len(c) > 0:
                superobject_properties = []
                for i in c:
                    if i.n3():
                        superobject_properties.extend(self.__get_direct_super_properties(i))
                c = superobject_properties
                depth += 1
        elif c1 in list(self.o.subjects(RDF.type, OWL.NamedIndividual)):
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

    def prof(self, _class: rdflib.URIRef):
        depth = 0
        if _class in list(self.o.subjects(RDF.type, OWL.Class)):
            if self.concept_profs.get(_class) is not None:
                return self.concept_profs[_class]
            depth = self.dist(_class, OWL.Thing)
            if self.storing:
                self.concept_profs[_class] = depth
        elif _class in list(self.o.subjects(RDF.type, OWL.ObjectProperty)):
            if self.relation_profs.get(_class) is not None:
                return self.relation_profs[_class]
            depth = self.dist(_class, OWL.topObjectProperty)
            self.relation_profs[_class] = depth
        
        return depth

    def taxonomic_property_similarity(self, x, y):
        set_x = self.get_superobject_properties(x, False)
        set_x.append(x)
        set_y = self.get_superobject_properties(y, False)
        set_y.append(y)

        lcs = self.prof_LCS(set_x, set_y, x, y)
        profLCS = self.prof(lcs)

        dxa = self.dist(x, lcs)
        dxroot = profLCS + dxa
        dya = self.dist(y, lcs)
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
    
    def get_types(self, ind, direct):
        classes = []
        clses = [o for s, p, o in self.o.triples((ind, RDF.type, None))]
        if direct:
            classes.extend(clses)
        else:
            for cls in clses:
                classes.extend(self.__get_direct_superclasses(cls))
            classes.extend(clses)
        
        return classes
    
    def get_OWL_relation(self, uri: str) -> OWLRelation:
        return self.relations.get(uri)
    
    def get_OWL_concept(self, uri: str) -> OWLConcept:
        con = self.concepts.get(uri)
        if con is None:
            con = OWLConcept(URIRef(uri), self)
            self.concepts[uri] = con
        
        return con
    
    def get_OWL_individual(self, uri: str) -> rdflib.URIRef:
        return URIRef(uri)
    
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
            lcs = self.prof_LCS(set_x, set_y, x, y)
            lcs_concept = self.get_OWL_concept(lcs.n3()[1:-1])
            # if self.storing:
            #     self.lcas[comparison] = lcs_concept
        
        return lcs_concept

    def dps(self, x: OWLConcept, y: OWLConcept):
        lcs = self.get_LCS(x, y)

        prof_LCS = self.prof(lcs.get_OWL_class())
        dxa = self.dist(x.get_OWL_class(), lcs.get_OWL_class())
        dya = self.dist(y.get_OWL_class(), lcs.get_OWL_class())
        dps = 1.0 - prof_LCS / (prof_LCS + dxa + dya)

        return 1.0 - dps

    def taxonomic_class_similarity(self, x: OWLConcept, y: OWLConcept):
        # dtax = self.dtax(x, y)
        dps = self.dps(x, y)

        return dps


class AnnSim():
    def __init__(self):
        self.v1 = None
        self.v2 = None
        self.cost_matrix = None
        self.assignment = 0
        self.map_comparisons = None

    def __init__(self, matrix):
        self.map_comparisons = matrix
    
    def matching(self, a: set, b: set, orig, des):
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
    
    def maximum_matching(self, a: set, b: set, orig, des):
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
            
            sim = 0
            for i in range(len(self.v1)):
                max_row = 0
                for j in range(len(self.v2)):
                    if max_row < self.cost_matrix[i, j]:
                        max_row = self.cost_matrix[i, j]
                sim += max_row
            for j in range(len(self.v2)):
                max_col = 0
                for j in range(len(self.v1)):
                    if max_col < self.cost_matrix[i, j]:
                        max_col = self.cost_matrix[i, j]
                sim += max_col
            return sim / (len(a) + len(b))


class OWLRelation():
    def __init__(self, property, onto):
        self.o = onto
        self.p = property
        self.uri = property.n3()[1:-1]
    
    def get_OWL_object_property(self):
        return self.p
    
    def __str__(self):
        return self.uri
    
    def similarity(self, r):
        return self.o.taxonomic_property_similarity(self.get_OWL_object_property(), r.get_OWL_object_property())


class OWLLink():
    def __init__(self, r: OWLRelation, b: MyOWLLogicalEntity, exp: list):
        self.relation = r
        self.destiny = b
        self.explanations = exp
    
    def __init__(self, r: OWLRelation, b: MyOWLLogicalEntity):
        self.relation = r
        self.destiny = b
        self.explanations = None
    
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
            sim_exp = 1
            sim = sim_tax_rel * sim_tax_des * sim_exp
            return sim
        except Exception as e:
            e.with_traceback()
        
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
        inform_c = 0
        lca = self.o.get_LCS(self, c)
        ic = InformationContent.get_instance()
        inform_c = ic.get_IC(lca)

        return inform_c
    
    def on_sim(self, c):
        tax_sim = self.taxonomic_similarity(c)
        sim = tax_sim
        neigh_sim = 1
        if tax_sim > 0:
            neigh_sim = self.similarity_neighbors(c)
        sim = tax_sim * neigh_sim

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
                sim = bpm.maximum_matching(self.neighbors, c.neighbors, self, c)
                return sim
            except Exception as e:
                e.with_traceback()
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
            if not self.satisfiable or not c.satisfiable:
                return 0
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
    
    def similarity_neighbors(self, c):
        pass
    
    def taxonomic_similarity(self, c):
        pass



# MyOWLOntology('ontologies/go.owl', '')
# MyOWLOntology('ontologies/owl.owl', '')
# MyOWLOntology('ontologies/OntologyID(Anonymous-720361).owl', '')
# MyOWLOntology('ontologies/mls.owl', '')
MyOWLOntology('ontologies/DBPedia_People_2016-07-22_19-50-12.nq', '')