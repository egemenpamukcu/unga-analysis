import requests
import io
import pandas as pd
import numpy as np
import json
import math
import pdf2image
import pytesseract
from mpi4py import MPI


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name = MPI.Get_processor_name()

raw_all = pd.read_json('raw_resolutions.json').sample(n=8)
n = math.ceil(len(raw_all) / 8)
partitions = [raw_all[i * n:(i + 1) * n] for i in range((len(raw_all) + n - 1) // n )]

for i in range(size):
	if rank == i:
		raw = partitions[i]
		x = 0
		errors = []
		for ind, k in raw.iterrows():
			x += 1
			if k['Text'] == '':
				print('rank:', rank, ' status:', x/len(raw), k['Resolution'])
				for t in range(2):
					try:
						url = k['url']
						req = requests.get(url)
						pages = pdf2image.convert_from_bytes(req.content)
						res = ''
						for page in pages:
							res += pytesseract.image_to_string(page)
						k['Text'] = res
						break
					except:
						if t == 1:
							errors.append(k['Resolution'])
						continue

		if len(errors) >= 1:
			with open('ocr_errors_{}.txt'.format(i), 'w') as err:
				json.dump(errors, err)
		raw.to_json('complete_corpus_raw_{}.json'.format(i))