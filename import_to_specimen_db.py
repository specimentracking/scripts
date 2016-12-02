import csv
import time
import requests
import json

"""
This script parses given csv file and creates all the samples contained.
The order and type of columns is as follows:

|| Participant IP || Baby Sex || DOB || Date of Collection || Sent to UP || Confirmation at UP || Notes || Barcode || type || dna extraction

"""

batch_name = '11_batch'
file_name = 'NewSamples24March2016.csv'

test_config = {
    'url': 'http://martenson.bx.psu.edu:8080/api/projects/f2db41e1fa331b3e/specimens?key=XXX',
    'check_url': 'http://martenson.bx.psu.edu:8080/api/projects/f2db41e1fa331b3e/check?key=XXX',
    'default_state': 'psu',
    'input_file': '../sample_sheets/' + batch_name + '/' + file_name,
    'output_file': '../sample_sheets/' + batch_name + '/all_created_unified_' + file_name,
    'new_barcodes_output_file': '../sample_sheets/' + batch_name + '/new_barcodes_unified_' + file_name,
    'last_used_barcode': 8046
}

prod_config = {
    'url': 'http://samples.galaxyproject.org/api/projects/f2db41e1fa331b3e/specimens?key=XXX',
    'check_url': 'http://samples.galaxyproject.org/api/projects/f2db41e1fa331b3e/check?key=XXX',
    'default_state': 'psu',
    'input_file': '../prod/' + batch_name + '/' + file_name,
    'output_file': '../prod/' + batch_name + '/all_created_unified_' + file_name,
    'new_barcodes_output_file': '../prod/' + batch_name + '/new_barcodes_unified_' + file_name,
    'last_used_barcode': 8046
}

# list containing all samples that WILL be created by the script
future_samples = []
# list containing samples that will ahve newly generated barcodes
new_barcodes = []
# list containing all samples that WERE created by the script
samples_created = []

""" DEFINE THE ENVIRONMENT """
config = prod_config
print config

# """ PARSE THE LOCATION """
# def parse_location( location_string ):
#     # location allowed values: fridge: `0-99`, shelf: `0-99`, rack: `0-99`, box: `0-99`, spot: `A-I` --- `1-9`; ALL COMBINATIONS ARE ALLOWED
#     parsed_location_1 = 'fridge_'
#     parsed_location_2 = ''

#     if len( location_string ) == 0:
#         return '', ''

#     location_splitted = [ x.strip() for x in location_string.split( ',' ) ]

#     if location_splitted[0] == '-80':
#         parsed_location_1 += '1'
#     elif location_splitted[0] == '-20':
#         parsed_location_1 += '2'
#     else:
#         print 'error while parsing fridge location: ' + str( location_string )
#         return '', ''

#     if location_splitted[1].split(' ')[0].lower() == 'shelf':
#         parsed_location_1 += '-shelf_' + location_splitted[1].split(' ')[1]
#     else:
#         print 'error while parsing shelf location: ' + str( location_string )
#         return '', ''

#     #  racks are not being used yet
#     parsed_location_1 += '-rack_0'

#     if location_splitted[2].split( ' ' )[ 0 ].lower() == 'box' and len( location_splitted[ 2 ].split( ' ' ) ) == 2:
#         parsed_location_1 += '-box_' + location_splitted[2].split(' ')[1]
#     elif location_splitted[2].split( ' ' )[ 0 ] == 'box' and len( location_splitted[ 2 ].split( ' ' ) ) == 3:
#         parsed_location_2 = parsed_location_1
#         parsed_location_1 += '-box_' + location_splitted[ 2 ].split( ' ' )[1] + '-spot_' + location_splitted[ 2 ].split( ' ' )[2]
#         parsed_location_2 += '-box_' + location_splitted[ 3 ].split( ' ' )[1] + '-spot_' + location_splitted[ 3 ].split( ' ' )[2]
#         return parsed_location_1, parsed_location_2
#     else:
#         print 'error while parsing box/spot location: ' + str( location_string )
#         return '', ''

#     if len(location_splitted[3]) == 2:
#         parsed_location_1 += '-spot_' + location_splitted[3]
#     elif location_splitted[3][:4] == 'box':
#         parsed_location_2 = parsed_location_1
#         parsed_location_1 += '-spot_' + location_splitted[3].split('-')[0]
#         parsed_location_2 += '-spot_' + location_splitted[3].split('-')[1]
#     elif len(location_splitted[3]) == 5:
#         parsed_location_2 = parsed_location_1
#         parsed_location_1 += '-spot_' + location_splitted[3].split('-')[0]
#         parsed_location_2 += '-spot_' + location_splitted[3].split('-')[1]
#     else:
#         print 'error while parsing spot location: ' + str( location_string )
#         return '', ''

#     return parsed_location_1, parsed_location_2


def create_derivate( date, location, parent_sample_data, encoded_parent_id ):
    """Create derivate from the given information."""
    print 'parent sample data'
    print parent_sample_data
    derived_sample = {}
    derived_sample_data = {}
    derived_sample_data['parent_id'] = encoded_parent_id
    derived_sample_data['family'] = parent_sample_data['family']
    derived_sample_data['participant_relationship'] = parent_sample_data.get( 'participant_relationship' )
    # if parent_sample_data.get( 'sex', None ):
    #    derived_sample_data['sex'] = parent_sample_data['sex']
    derived_sample_data['date_of_collection'] = date
    derived_sample_data['location'] = location
    derived_sample_data['type'] = parent_sample_data['type'] + '-dna'
    derived_sample_data['state'] = config[ 'default_state' ]
    derived_sample['sample_data'] = derived_sample_data
    return derived_sample


def check_conflict( barcode ):
    time.sleep( 0.1 )
    param = '&barcode=' + str( barcode )
    response = requests.get( config[ 'check_url' ] + param )
    if response.status_code == 404:
        return False
    elif response.status_code == 200:
        print 'XXXXXXXXXXXXXXXXXXXXXXXX'
        print 'conflict detected for barcode: ' + str( barcode )
        return True


def post_specimen( specimen ):
    """
    Create request for the given specimen and send it.
    """
    if specimen is None:
        print ' ERROR - CANNOT POST EMPTY SPECIMEN'
        return
    param = '&barcode=' + specimen[ 'barcode' ]
    sample_data = specimen[ 'sample_data' ]
    payload = json.dumps( { 'sample_data': sample_data } )
    headers = { 'Content-type': 'application/json' }
    response = requests.post( config[ 'url' ] + param, data=payload, headers=headers )
    data = response.json()
    status_code = response.status_code
    print '*************************'
    print str( payload )
    if status_code == 409:
        print '******ERROR!*************'
        print str( data )
    elif status_code == 200:
        print 'CREATED A SAMPLE WITH THE FOLLOWING BARCODE: ' + specimen[ 'barcode' ]
        print str( data )
    else:
        print '******UNKWNOWN ERROR!*************'
        print 'STATUS CODE: ' + str( status_code )
        print str( data )

    time.sleep(0.5)
    samples_created.append( data )
    return data

with open( config[ 'input_file' ], 'rbU' ) as csvfile:
    """
    Open the input file and parse the contents.
    Expected format:
    || Participant IP || Baby Sex || DOB || Date of Collection || Sent to UP || Confirmation at UP || Notes || Barcode || type || DNA Isolation Date

    """
    spamreader = csv.reader( csvfile, dialect='excel', delimiter=',' )
    error = False

    last_generated_barcode = config[ 'last_used_barcode' ]

    for i, row in enumerate( spamreader ):
        sample = {}
        sample_data = {}

        col1 = str( row[ 0 ] ).strip()  # participant ip
        # print 'col1: ' + col1
        col2 = str( row[ 1 ] ).strip()  # baby sex
        # print 'col2: ' + col2
        col3 = str( row[ 2 ] ).strip()  # dob
        # print 'col3: ' + col3
        col4 = str( row[ 3 ] ).strip()  # doc
        # print 'col4: ' + col4
        col5 = str( row[ 4 ] ).strip()  # sent
        # print 'col5: ' + col5
        col6 = str( row[ 5 ] ).strip()  # confirmed
        # print 'col6: ' + col6
        col7 = str( row[ 6 ] ).strip()  # notes
        # print 'col7: ' + col7
        col8 = str( row[ 7 ] ).strip()  # barcode
        # print 'col8: ' + col8
        # col9 = str( row[ 8 ] ).strip()  # freezer location
        # print 'col9: ' + col9
        col10 = str( row[ 8 ] ).strip()  # type
        # print 'col10: ' + col10
        dna_extraction_date = str( row[ 9 ] ).strip()  # extraction date

        # Participant IP
        # participant_ip = str( row[ 1 ] ).strip()
        if len( col1[:3] ) > 0:
            sample_data[ 'family' ] = col1[:3]
        if len( col1[3:].strip() ) > 0:
            sample_data[ 'participant_relationship' ] = col1[3:].strip()

        # Baby Sex
        if ( len( col2 ) > 0 and ( col2.upper() == 'F' or col2.upper() == 'M') ):
            sample_data['sex'] = col2.upper()

        # DOB
        if len( col3 ) > 0:
            dob_array = col3.split( '/' )
            full_dob = dob_array[ 0 ] + '/' + dob_array[ 1 ] + '/' + dob_array[ 2 ]
            sample_data[ 'participant_dob' ] = full_dob

        # Date of Collection
        if len( col4 ) > 0:
            doc_array = col4.split( '/' )
            full_doc = doc_array[ 0 ] + '/' + doc_array[ 1 ] + '/' + doc_array[ 2 ]

        # # Date Sent to UP
        if len( col5 ) > 0:
            sent_array = col5.split( '/' )
            full_sent = sent_array[ 0 ] + '/' + sent_array[ 1 ] + '/' + sent_array[ 2 ]
            sample_data[ 'date_sent' ] = full_sent

        # # Confirmation at UP
        if len( col6 ) > 0:
            confirm_array = col6.split( '/' )
            full_confirm = confirm_array[ 0 ] + '/' + confirm_array[ 1 ] + '/' + confirm_array[ 2 ]
            sample_data[ 'date_confirmation' ] = full_confirm

        # Note
        sample_data[ 'note' ] = col7

        # Is expended?
        # is_expended = ( len( col9 ) > 0 and col10 == 'buccal' )
        # if is_expended:
        #     sample_data[ 'state' ] = 'depleted'
        # else:
        sample_data[ 'state' ] = config[ 'default_state' ]

        # Just generate Barcode
        # generated_barcode = last_generated_barcode + 1
        # last_generated_barcode = generated_barcode
        # sample[ 'barcode' ] = str( generated_barcode )
        # new_barcodes.append( sample[ 'barcode' ] )

        # Barcode Can be present
        barcode = col8
        if barcode != '':
            try:
                barcode = int( barcode )
            except Exception, e:
                error = True
                print 'Unable to cast Barcode to integer: ' + str( barcode )
            sample[ 'barcode' ] = str( barcode )
        else:
            # no barcode specified, we have to generate one
            generated_barcode = last_generated_barcode + 1
            last_generated_barcode = generated_barcode
            sample[ 'barcode' ] = str( generated_barcode )
            new_barcodes.append( sample[ 'barcode' ] )

        # Location
        # parsed_location_1, parsed_location_2 = parse_location( location )
        # if parsed_location_2 == '':
        #     if parsed_location_1 != '':
        #         sample_data['location'] = parsed_location_1
        # else:
        #     error = True
        #     print 'error while parsing location, first location column should never contain two locations!'

        # if len( col9 ) > 0:
        #     parsed_location_1, parsed_location_2 = parse_location( col9 )
        #     if parsed_location_1 != '':
        #         sample_data['location'] = parsed_location_1
        #     else:
        #         error = True
        #         print 'error while parsing location'

        # Parent ID
        # parent_id = str( row[ 2 ] ).strip()
        # if parent_id:
        #     sample_data[ 'parent_id' ] = parent_id
        col10 = col10.lower().replace( ' ', '_' )
        if col10 in [ 'blood', 'buccal', 'hair', 'breastmilk', 'stool', 'vaginal_swab', 'placenta', 'cord_blood', 'tissue', 'rectal_swab', 'skin_swab' ]:
            sample_data[ 'type' ] = col10
        else:
            error = True
            print 'Unknown type: ' + str( col10 )

        if not error:
            sample[ 'sample_data' ] = sample_data
            future_samples.append( { 'sample': sample, 'derivate_extract_date': dna_extraction_date } )
            # future_samples.append( { 'sample': sample } )

    conflict = False
    for future_sample in future_samples:
        conflict = check_conflict( future_sample[ 'sample' ][ 'barcode' ] ) or conflict

    if not conflict:
        derivates_counter = 0
        print 'Starting to create the following number of specimens: ' + str( len( future_samples ) )
        for future_sample in future_samples:

            data = post_specimen( future_sample[ 'sample' ] )

        #     """ DERIVATE DETECTION AND CREATION """
            encoded_sample_id = data[ 'id' ]
            derivate1 = None

            if len( future_sample[ 'derivate_extract_date' ] ) > 0:
                derivate1 = create_derivate( future_sample[ 'derivate_extract_date' ], None, data[ 'sample_data' ], encoded_sample_id )
                derivates_counter += 1
                generated_barcode = last_generated_barcode + 1
                last_generated_barcode = generated_barcode
                derivate1[ 'barcode' ] = str( generated_barcode )
                new_barcodes.append( derivate1[ 'barcode' ] )

            # POST derivates_counter
            if derivate1 is not None:
                derivate1_data = post_specimen( derivate1 )
        print 'number of created derivates_counter: ' + str( derivates_counter )

with open( config[ 'output_file' ], 'wb' ) as csvfile:
    """ Save all the responses in csv file for error checking """
    spamwriter = csv.writer( csvfile )
    print 'total num of samples created: ' + str( len( samples_created ) )
    for idx, sample in enumerate( samples_created ):
            row = []
            row.append( sample[ 'bar_code' ] )
            sample_data = sample[ 'sample_data' ]
            if sample_data.get('family', None) and sample_data.get('participant_relationship', None):
                row.append( sample_data[ 'family' ] + sample_data[ 'participant_relationship' ] )
            row.append( sample[ 'id' ] )
            row.append( str( sample ) )
            spamwriter.writerow( row )
with open( config[ 'new_barcodes_output_file' ], 'wb' ) as barcode_file:
    barcodewriter = csv.writer( barcode_file)
    for idx, sample in enumerate( samples_created ):
        if sample[ 'bar_code' ] in new_barcodes:
            sample_data = sample[ 'sample_data' ]
            row = []
            row.append( sample[ 'bar_code' ] )
            row.append( sample[ 'id' ] )
            row.append( sample_data[ 'type' ] )
            if sample_data.get('family', None) and sample_data.get('participant_relationship', None):
                row.append( sample_data[ 'family' ] + sample_data[ 'participant_relationship' ] )
            row.append( str( sample ) )
            barcodewriter.writerow( row )

print 'new barcodes: '
print len( new_barcodes )
print str( new_barcodes )
print 'THIS IS ALL FOLKS!'
