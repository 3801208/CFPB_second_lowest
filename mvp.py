# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 10:48:38 2021

@author: 3801208
"""
import pandas as pd

def create_new_output_file(slcsp_filename):
    """Function to create an output CSV file"""
    try:
        f = open(slcsp_filename, "w")
    except:
        print("Error creating the file: {}".format(slcsp_filename))
        print("retrying....")
        return 1, False

    f.write("zipcode,rate\n")

    return f, True

###############################################################################

def get_file_name(f_type):
    """Function to read the 3 input files (slcsp - list of zip codes to use for the lookup,
    zips - list of zip codes, rate plans - list of plans and prices) from the user"""


    new_file = ''

    try:
        if f_type == 'slcsp':
            filename=input('Enter the full path and filename for the CSV file listing the zip codes to determine the slcsp (slcsp.csv): ')

            #set up the column types so that the zipcode defaults to a string type
            types_dict = {'zipcode': str, 'rate': str}
            df = pd.read_csv(filename, dtype=types_dict)

            #set up the new output file
            new_file_path=filename.split(".csv")
            new_file=new_file_path[0] + "_upd.csv"

            #check number of columns in the file
            if len(df.columns) != 2:
                print("This file does not have the expected number of columns")
                raise Exception

            #check that the file format matches what is expected
            expected_cols = ['zipcode', 'rate']
            if set(df.columns) != set(expected_cols):
                 print("The format of this file does not match what is expected")
                 raise Exception


        elif f_type == 'Zip Code':
            filename=input('Enter the full path and filename for the CSV file listing the zip codes and rate area information(zips.csv): ')
            #set up the column types so that the zipcode defaults to a string type
            types_dict = {'zipcode': str, 'state': str, 'county_code': str, 'name': str, 'rate_area': int}
            df = pd.read_csv(filename, dtype=types_dict)

            #check number of columns in the file
            if len(df.columns) != 5:
                print("This file does not have the expected number of columns")
                raise Exception

            #check that the file format matches what is expected
            expected_cols = ['zipcode', 'state', 'county_code', 'name', 'rate_area']
            if set(df.columns) != set(expected_cols):
                 print("The format of this file does not match what is expected")
                 raise Exception

        else:
            filename=input('Enter the full path and filename for the CSV file listing the plans(plans.csv): ')
            df = pd.read_csv(filename)

            #check number of columns in the file
            if len(df.columns) != 5:
                print("This file does not have the expected number of columns")
                raise Exception

            #check that the file format matches what is expected
            expected_cols = ['plan_id', 'state', 'metal_level', 'rate', 'rate_area']
            if set(df.columns) != set(expected_cols):
                 print("The format of this file does not match what is expected")
                 raise Exception

    except FileNotFoundError:
        print('The path and file - {} - entered was not found'.format(filename))
        print('Please re-verify your input file for {}\n'.format(f_type))
        return False, False, False

    except:
        print('Please re-verify your input file for {}\n'.format(f_type))
        return False, False, False

    print()
    return df, new_file, True

###############################################################################

def process_slcsp(df_slcsp, df_zip, df_rate_plan, file_ptr):
    """Function to process the zip codes to identify the slcsp
       or print a warning stating why the slcsp was not identified"""

    #step through the zip codes in the slcsp dataframe
    for x in range(len(df_slcsp)):

        ######################################################################
        #several checks to make sure that the zip code is valid
        ######################################################################

        #ensure that the zip code is represented as a 5 digit string - to handle leading 0's
        if df_slcsp.loc[x].iat[0].isnumeric():
            zip_search="{:05d}".format(int(df_slcsp.loc[x].iat[0]))

            #if the zip is not 5 digits, do not process
            if len(zip_search) != 5:
                print('Warning: Zip code {} not 5 digits - not processing this zip code'.format(zip_search))
                file_ptr.write("{},\n".format(zip_search))
                continue

        #The zip code is not numeric, do not process
        else:
           print('Warning: Zip code {} not numeric - not processing this zip code'.format(df_slcsp.loc[x].iat[0]))
           file_ptr.write("{},\n".format(df_slcsp.loc[x].iat[0]))
           continue


        #create a temp dataframe with only the records that match the zip code
        #that is being searched
        df_temp = df_zip[df_zip["zipcode"] == zip_search]

        #check to if the zip code was found
        if df_temp.empty:
            print('Warning: Zip code {} was not found in list of zip codes - not processing this zip code'.format(zip_search))
            file_ptr.write("{zip_code},\n".format(zip_code=zip_search))
            continue

        #update the index for the df - delete the existing index and create a new index
        df_temp = df_temp.reset_index(drop=True)
        #get the rate_areas and the state for that zip code
        rate_area_temp = df_temp["rate_area"]
        state_from_zip_lookup = df_temp.loc[0].at['state']


        # use a set to get the unique rate_areas and test to see if they are all the same
        #if there are multiple rate areas, the rate cannot be determined -> set output to ''
        if len(set(rate_area_temp)) > 1:
            print('Warning: Zip code {} has Multiple Rate Areas'.format(zip_search))
            slcsp_rate = ''
            file_ptr.write("{zip_code},{rate_plan}\n".format(zip_code=zip_search, rate_plan=slcsp_rate))
            continue


        #find the second lowest cost silver plan
        #use the state and rate area to get the list of matches from the rate_plan and
        #create a temp dataframe - then sort and re-index the temp df
        temp_df_rate_plan = df_rate_plan[(df_rate_plan["state"] == state_from_zip_lookup) & 
                                    (df_rate_plan["rate_area"] == rate_area_temp[0])]
        temp_df_rate_plan = temp_df_rate_plan.sort_values(by=['rate'])
        temp_df_rate_plan = temp_df_rate_plan.reset_index(drop=True)

        #make sure that there is more than 1 plan:
        #if there are 2+ plans, get the second one and format the output to two decimal places
        #If less than 2 plans, print a warning and set the rate to ''
        if len(temp_df_rate_plan) < 2:
            print('Warning: Zip code {} only has one Silver Plan found'.format(zip_search))
            slcsp_rate = ''
            file_ptr.write("{zip_code},{rate_plan}\n".format(zip_code=zip_search, rate_plan=slcsp_rate))
            continue

        slcsp_rate = "{:.2f}".format(temp_df_rate_plan.loc[1].at['rate'])


        #write out the zipcode/rate information to the new CSV file
        file_ptr.write("{zip_code},{rate_plan}\n".format(zip_code=zip_search, rate_plan=slcsp_rate))

    #completed processing all of the entries
    #close the file for writing
    file_ptr.close()
    return 0

###############################################################################

def print_final(slcsp_filename):
    """Function that opens the newly created file and then prints the contents"""

    file_ptr = open(slcsp_filename, "r")
    print('\n\nFinal output from new file:[{}]\n'.format(slcsp_filename))
    print(file_ptr.read())
    file_ptr.close()
    return 0

##############################################################################
# main function to identify the slcsp
##############################################################################
if __name__ == "__main__":
    """the main function for the application.  This function calls other
    helping functions to determine the slcsp for the zip codes given -
    once processing is complete, the new output file is read and displayed
    on stdout"""

    torf = False

    #get the input file for the slcsp file
    while torf == False:
        df_slcsp, slcsp_filename, torf = get_file_name('slcsp')



    torf = False

    #get the input file for the zip_code file
    while torf == False:
        df_zip, zip_filename, torf = get_file_name('Zip Code')

    torf = False

    #get the input file for the rate_plan file
    while torf == False:
        df_rate_plan1, rate_filename, torf = get_file_name('rate_plan')

    #Get the Silver plans only
    df_rate_plan = df_rate_plan1[df_rate_plan1["metal_level"] == 'Silver']


    #call fn to create the new CSV file
    #if the file cannot be created try 10 times then exit
    cnt = 0
    torf = False
    while torf == False &  cnt < 10:
        cnt += 1
        file_ptr, torf = create_new_output_file(slcsp_filename)

    if torf == True & cnt < 10:
        #call to function to process the inputs
        rc = process_slcsp(df_slcsp, df_zip, df_rate_plan, file_ptr)

        #call to function to print out the final results
        rc = print_final(slcsp_filename)
    else:
        print("The output file could not be created.  Please try again")

    ans = input("Processing complete. Please press enter to exit")
