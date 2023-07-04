import os
# from myontology import *

def similarity2csv(v1: list, v2: list=None, sim_type='similarity', file=None, cartesian=True):
    str2write = 'Entity,Entity,Similarity\r\n'
    if v2 is None:
        for i in v1:
            for j in v1:
                str2write += str(i) + ',' + str(j) + ','
                if sim_type == 'similarity':
                    str2write += str(i.similarity(j)) + '\r\n'
                elif sim_type == 'taxonomic_similarity':
                    str2write += str(i.taxonomic_similarity(j)) + '\r\n'
                elif sim_type == 'similarity_neighbors':
                    str2write += str(i.similarity_neighbors(j)) + '\r\n'
    else:
        if cartesian:
            for i in v1:
                for j in v2:
                    str2write += str(i) + ',' + str(j) + ','
                    if sim_type == 'similarity':
                        str2write += str(i.similarity(j)) + '\r\n'
                    elif sim_type == 'taxonomic_similarity':
                        str2write += str(i.taxonomic_similarity(j)) + '\r\n'
                    elif sim_type == 'similarity_neighbors':
                        str2write += str(i.similarity_neighbors(j)) + '\r\n'
        else:
            for i, j in zip(v1, v2):
                str2write += str(i) + ',' + str(j) + ','
                if sim_type == 'similarity':
                    str2write += str(i.similarity(j)) + '\r\n'
                elif sim_type == 'taxonomic_similarity':
                    str2write += str(i.taxonomic_similarity(j)) + '\r\n'
                elif sim_type == 'similarity_neighbors':
                    str2write += str(i.similarity_neighbors(j)) + '\r\n'
    if file is None:
        with open('results/' + sim_type + '.csv', 'wt') as f:
            f.write(str2write)
    else:
        with open(file, 'wt') as f:
            f.write(str2write)

# generate all graph files that are mandatory for semEP
def generate_bigraph(vertices1: list, vertices2: list, edges: list, outdir='/mnt/c/Users/SongZ/Downloads/repositories/semep/test/p4lucat/'):
    """
    Generate all graph files that are mandatory for semEP
    :param vertices1: a list contains elements of type MyOWLLogicalEntity
    :param vertices2: a list contains elements of type MyOWLLogicalEntity
    :param edges: a list conatins tuples, which describe the edge between vertices
                    for example: [(vertex in vertices1, vertex in vertices2, weight of edge)]
    :param outdir: the directory of output files
    
    :return: None
    """

    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # path of all necessary files
    vertices1_file = outdir + 'vertices1.txt'
    vertices1_simmat_file = outdir + 'vertices1_simmat.txt'
    vertices2_file = outdir + 'vertices2.txt'
    vertices2_simmat_file = outdir + 'vertices2_simmat.txt'
    bipartite_graph_file = outdir + 'bigraph.txt'

    with open(vertices1_file, 'wt') as v1f:
        v1f.write('{:d}\n'.format(len(vertices1)))
        for v1 in vertices1[:-1]:
            v1f.write('{}\n'.format(str(v1)))
        v1f.write('{}'.format(str(vertices1[-1])))
    
    with open(vertices1_simmat_file, 'wt') as v1sf:
        v1sf.write('{:d}\n'.format(len(vertices1)))
        for v11 in vertices1:
            for v12 in vertices1:
                sim = v11.similarity(v12)
                v1sf.write('{:.6f} '.format(sim))
                # v1sf.write('{:.6f} '.format(1.0))
                # v1sf.write('{:.6f} '.format(v11.taxonomic_similarity(v12)))
                print(str(v11), str(v12), sim)
            v1sf.write('\n')
    
    with open(vertices2_file, 'wt') as v2f:
        v2f.write('{:d}\n'.format(len(vertices2)))
        for v2 in vertices2[:-1]:
            v2f.write('{}\n'.format(str(v2)))
        v2f.write('{}'.format(str(vertices2[-1])))
    
    with open(vertices2_simmat_file, 'wt') as v2sf:
        v2sf.write('{:d}\n'.format(len(vertices2)))
        for v21 in vertices2:
            for v22 in vertices2:
                sim = v21.similarity(v22)
                v2sf.write('{:.6f} '.format(sim))
                # v2sf.write('{:.6f} '.format(1.0))
                # v2sf.write('{:.6f} '.format(v21.taxonomic_similarity(v22)))
                print(str(v21), str(v22), sim)
            v2sf.write('\n')
    
    with open(bipartite_graph_file, 'wt') as bgf:
        bgf.write('{:d}\n'.format(len(edges)))
        for v1, v2, w in edges:
            if w >= 0.0:
                bgf.write('{}\t{}\t{:.8f}\n'.format(str(v1), str(v2), w))