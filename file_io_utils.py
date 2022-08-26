import os, sys
import json, zipfile

def write_zipped_json(data: dict, attributes: dict, zip_file_path: str):

    # create a data structure with data and attrbutes
    to_save = {'attributes': attributes, 'data':data}
    if not zip_file_path.endswith('.zip'):
        raise Exception('zip file name must has .zip extension')


    with zipfile.ZipFile(zip_file_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file: 

        for fn, content in to_save.items():
            dumped_json = json.dumps(content, ensure_ascii=False, indent=4)
            zip_file.writestr(fn + '.json', data=dumped_json)
        
        zip_file.testzip()

def read_zipped_json(file_path, attrib_only = False):

    with zipfile.ZipFile(file_path, 'r') as zip_file:
        data = json.loads(zip_file.read('data.json')) if not attrib_only else None
        attribs = json.loads(zip_file.read('attributes.json'))

    return attribs, data