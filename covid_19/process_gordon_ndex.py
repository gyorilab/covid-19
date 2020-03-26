import pickle
from indra.sources import ndex_cx

cxp = ndex_cx.process_ndex_network('43803262-6d69-11ea-bfdc-0ac135e8bacf',
                                   require_grounding=False)

with open('../gordon_ndex_stmts.pkl', 'wb') as fh:
    pickle.dump(cxp.statements, fh)
