import os
from collections import Counter
from indra.sources import reach
from indra_db.util.content_scripts import TextContentSessionHandler
from indra.statements import stmts_from_json_file


BASE_PATH = '/Users/ben/data/reach_grounding_eval'
PAPERS_PATH = os.path.join(BASE_PATH, 'text')
OLD_REACH_PATH = os.path.join(BASE_PATH, 'old_reach')
NEW_REACH_PATH = os.path.join(BASE_PATH, 'new_reach')


def get_top_papers_by_uppro(stmts):
    papers = []
    for stmt in stmts:
        for agent in stmt.real_agent_list():
            if 'UPPRO' in agent.db_refs:
                for ev in stmt.evidence:
                    trid = ev.text_refs.get('TRID')
                    if trid:
                        papers.append(trid)
    return Counter(papers)


def get_top_papers_by_sars_cov_2(stmts):
    from covid_19.process_gordon_ndex import mappings
    papers = []
    for stmt in stmts:
        for agent in stmt.real_agent_list():
            if agent.db_refs.get('TEXT') in mappings:
                for ev in stmt.evidence:
                    trid = ev.text_refs.get('TRID')
                    if trid:
                        papers.append(trid)
    return Counter(papers)


def fetch_text_for_papers(trids, cached=True):
    tcs = TextContentSessionHandler()
    contents = {}
    for trid in trids:
        print('Fetching %s' % trid)
        fname = os.path.join(PAPERS_PATH, str(trid))
        if not cached or not os.path.exists(fname):
            content = \
                tcs.get_text_content_from_text_refs({'TRID': trid})
            if content is None:
                continue
            content = content.strip()
            with open(fname, 'w') as fh:
                fh.write(content)
        else:
            with open(fname, 'r') as fh:
                content = fh.read()
        contents[trid] = content
    return contents


def run_reading(text_contents, cached=True):
    organism_preference = None
    stmts = {}
    for trid, text_content in text_contents.items():
        print('Reading %s' % trid)
        output_fname = os.path.join(NEW_REACH_PATH, '%s.json' % trid)
        if cached and os.path.exists(output_fname):
            rp = reach.process_json_file(output_fname)
            if rp is None:
                continue
        else:
            if text_content.startswith('<!DOCTYPE'):
                rp = reach.process_nxml_str(text_content,
                                            url=reach.local_nxml_url,
                                            output_fname=output_fname,
                                            organism_priority=organism_preference)
            else:
                rp = reach.process_text(text_content,
                                        url=reach.local_text_url,
                                        output_fname=output_fname,
                                        organism_priority=organism_preference)
        if rp is not None:
            stmts[trid] = rp.statements
    return stmts


if __name__ == '__main__':
    #stmts_file = os.path.join(os.pardir, 'stmts',
    #                          'statements_2020-12-07-18-06-18')
    #stmts = stmts_from_json_file(stmts_file)
    #top_papers = get_top_papers(stmts)
    #fetch_text_for_papers([x[0] for x in top_papers.most_common(10)])
    #trid_list = [31796193, 32107916, 30654634, 29753886, 26812907,
    #             31313896, 31668659, 31477780, 29901482, 22628647]
    #trid_list = [
    #    32513140,
    #    32043768,
    #    28305290,
    #    32137481,
    #    8794951,
    #    32451367,
    #    5823107,
    #    21467044,
    #    16491859,
    #    4449248]
    text_contents = fetch_text_for_papers(trid_list, cached=True)
    stmts = run_reading(text_contents, cached=True)
