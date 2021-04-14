import requests
import io
import re
import pandas as pd
import numpy as np
import json
import shutil
from mpi4py import MPI
import math
import pdftotext

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()

def create_corpus(pdf_urls):
    '''
    Iterates through URLs directing to UNGA Resolution PDFs, converts them to string,
    and stores them in a DataFrame. The resulting DataFrame will be incomplete because
    some of the PDFs have images of scanned documents instead of text. To get the text
    of these resolutions through Tesseract Optical Character Recognition engine, run
    complete_corpus_ocr.py using the output of this file as an input.
    '''

    #pdf_urls = [[k, v] for k, v in all_pdfs.items()]
    for i, pdf_url in enumerate(pdf_urls):
        req = requests.get(pdf_url[1])
        with io.BytesIO(req.content) as f:
            try:
                pdf = pdftotext.PDF(f)
            except pdftotext.Error:
                print(pdf_url[0], 'ERRORED OUT')
                continue
        text = ''
        for page in pdf:
            text += page
        pdf_url.append(text.strip())
        print('rank {}:'.format(rank), str((round((i + 1)/len(pdf_urls)*100, 4))) + '%')
    return pd.DataFrame(pdf_urls, columns=['Resolution', 'url', 'Text'])


with open('all_pdfs.txt') as pdfs:
    all_pdfs = json.load(pdfs)

pdf_urls = [[k, v] for k, v in all_pdfs.items()]

n = math.ceil(len(pdf_urls) / size)
partitions = [pdf_urls[i * n:(i + 1) * n] for i in range((len(pdf_urls) + n - 1) // n )]

for i in range(size):
    if rank == i:
        corpus = create_corpus(partitions[i])
        corpus.to_json('raw_corpus_{}.json'.format(i))


