import pickle
from collections import Counter
from indra.util import write_unicode_csv
from indra.statements import Complex
from indra.databases import get_identifiers_url
from indra.preassembler.grounding_mapper.standardize import name_from_grounding


# Load statements
with open('../cord19_pmc_stmts_filt.pkl', 'rb') as fh:
    stmts = pickle.load(fh)

texts = []
ev_text_for_agent_text = {}
# Iterate over each statement and its agents
for stmt in stmts:
    for idx, agent in enumerate(stmt.agent_list()):
        if agent is None:
            continue
        # Get some properties of the assembled agent (grounding, standard name,
        # link-out URL)
        gr = agent.get_grounding()
        standard_name = name_from_grounding(*gr) if gr[0] is not None else ''
        url = get_identifiers_url(*gr) if gr[0] is not None else ''
        # For Complexes, we can't rely on the indexing in annotations to get
        # the raw_text of the Agents so we just take the surface-level
        # TEXT entry in the assembled Agent's db_refs
        if isinstance(stmt, Complex):
            agent_texts = [agent.db_refs['TEXT']] if 'TEXT' in agent.db_refs \
                else []
        # In other cases, we iterate over all the evidences and use the agent
        # index to find the raw text in annotations
        else:
            agent_texts = []
            for ev in stmt.evidence:
                if 'agents' not in ev.annotations:
                    continue
                raw_txt = ev.annotations['agents']['raw_text'][idx]
                if raw_txt:
                    agent_texts.append(raw_txt)
        for t in agent_texts:
            if not t:
                import ipdb; ipdb.set_trace()
            # These entries are presumably overwritten many times but
            # that's okay.
            ev_text_for_agent_text[t] = (ev.pmid, ev.text)
            # We now add a new entry to the text-grounding list
            texts.append((t, ('%s:%s' % gr) if gr[0] else '', standard_name,
                          url))

# Count the unique text-grounding entries
cnt = Counter(texts)

# Dump the results into a TSV file
rows = [['text', 'grounding', 'standard_name', 'url', 'count',
         'pmid', 'ev_text']]
for data, count in cnt.most_common():
    pmid, ev_text = ev_text_for_agent_text[data[0]]
    row = list(data) + [str(count), pmid, ev_text]
    rows.append(row)
write_unicode_csv('../grounding_table.tsv', rows, delimiter='\t')
