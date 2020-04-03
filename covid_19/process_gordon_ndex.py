from os.path import join, dirname, abspath
import pickle
from indra.sources import ndex_cx

stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                  'gordon_ndex_stmts.pkl')

cxp = ndex_cx.process_ndex_network('43803262-6d69-11ea-bfdc-0ac135e8bacf',
                                   require_grounding=False)

with open(stmts_file, 'wb') as fh:
    pickle.dump(cxp.statements, fh)
