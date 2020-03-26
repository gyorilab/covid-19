import csv


def read_metadata(fname):
    entries = []
    with open(fname, 'r') as fh:
        reader = csv.reader(fh)
        header = next(reader)
        header[0] = 'ID'
        for row in reader:
            entry = {h: str(v) for h, v in zip(header, row)}
            entries.append(entry)
    return entries


def get_text_refs_from_metadata(entry, metadata_version='1'):
    mappings = {
        'ID': 'CORD19_INDRA_V%s' % metadata_version,
        'sha': 'CORD19_SHA',
        'doi': 'DOI',
        'pmcid': 'PMCID',
        'pubmed_id': 'PMID',
        'WHO #Covidence': 'WHO_COVIDENCE',
        'Microsoft Academic Paper ID': 'MICROSOFT'
    }
    text_refs = {}
    for key, ref_key in mappings.items():
        val = entry.get(key)
        if val:
            # Temporary patch to remove float suffixes
            if val.endswith('.0'):
                val = val[:-2]
            text_refs[ref_key] = val
    return text_refs
