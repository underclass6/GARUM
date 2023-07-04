from os import listdir
import time
from myontology import MyOWLOntology, AnnSim, AnnotationComparison


class DatasetTest():
    @staticmethod
    def read_comparison_file(comparison_file: str) -> list:
        comparisons = []
        with open(comparison_file, 'rt') as f:
            line = f.readline()
            while line:
                elements = line.split('\t')
                elements[0] = elements[0].replace('\r', '')
                elements[0] = elements[0].replace('\n', '')
                elements[1] = elements[1].replace('\r', '')
                elements[1] = elements[1].replace('\n', '')
                comparisons.append(ComparisonResult(elements[0], elements[1]))
                line = f.readline()
        
        return comparisons
    
    @staticmethod
    def get_concept_annotations(concept_name: str, folder: str, o: MyOWLOntology) -> list:
        annotations = set()
        with open(folder + '/' + concept_name, 'rt') as f:
            line = f.readline()
            while line:
                term = line.split('\t')[0]
                term = term.replace('\r', '')
                term = term.replace('\n', '')
                if o.get_ontology_prefix() not in term:
                    term = o.get_ontology_prefix() + term.replace(':', '_')
                c = o.get_my_OWL_logical_entity(term)
                if c is not None:
                    annotations.add(c)
                line = f.readline()
        
        return list(annotations)


class ComparisonResult():
    def __init__(self, a: str, b: str):
        self.concept_A = a
        self.concept_B = b
        self.similarity = -1.0
    
    def __str__(self):
        return self.concept_A + '\t' + self.concept_B + '\t' + str(self.similarity)
    
    def get_concept_A(self) -> str:
        return self.concept_A
    
    def get_concept_B(self) -> str:
        return self.concept_B
    
    def set_similarity(self, s):
        self.similarity = s
    
    def get_similarity(self):
        return self.similarity
    
    def __eq__(self, b: object) -> bool:
        if isinstance(b, ComparisonResult):
            return self.concept_A == b.concept_A \
                and self.concept_B == b.concept_B \
                    or self.concept_A == b.concept_B \
                        and self.concept_B == b.concept_A
        return False


def run_test():
    prefixs = ['resources/dataset3/']
    for prefix in prefixs:
        # ont_file = prefix + 'goProtein/go.owl'
        ont_file = 'ontologies/go.owl'
        o = MyOWLOntology(ont_file, pr='http://purl.org/obo/owl/GO#')

        comparison_file = prefix + 'proteinpairs.txt'
        comparisons = DatasetTest.read_comparison_file(comparison_file)

        files = [prefix + 'bp_annt_2008']
        for f in files:
            entities = set()
            p_names = listdir(f)
            entities.update(p_names)
        
        concept_comparisons = set()
        for comp in comparisons:
            for file in files:
                a = DatasetTest.get_concept_annotations(comp.get_concept_A(), file, o)
                b = DatasetTest.get_concept_annotations(comp.get_concept_B(), file, o)
                for c1 in a:
                    for c2 in b:
                        concept_comparisons.add(AnnotationComparison(c1, c2))
        
        cost_matrix = {}
        index = 0
        tic = time.perf_counter()
        for comparison in list(concept_comparisons):
            sim = comparison.get_concept_A().similarity(comparison.get_concept_B())
            cost_matrix[comparison] = sim
            index += 1
            if index % 100 == 0:
                print(index, time.perf_counter() - tic)

        bpm = AnnSim(cost_matrix)

        with open(prefix + 'results.txt', 'wt') as f:
            for comp in comparisons:
                for file in files:
                    a = DatasetTest.get_concept_annotations(comp.get_concept_A(), file, o)
                    b = DatasetTest.get_concept_annotations(comp.get_concept_B(), file, o)
                    try:
                        sim = bpm.matching(a, b, None, None)
                    except Exception:
                        continue
                    comp.set_similarity(sim)
                    f.write(str(comp) + '\r\n')
                    print(comp)

if __name__ == '__main__':
    run_test()