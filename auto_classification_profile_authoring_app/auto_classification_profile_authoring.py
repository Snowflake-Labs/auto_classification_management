# Import python packages
import streamlit as st
import json
from snowflake.snowpark.context import get_active_session

@st.cache_data
def get_tags():
    get_tags_sql = "show tags in account"
    rows = session.sql(get_tags_sql).collect()
    tags = []
    for row in rows:
        tag_name = row["name"]
        database_name = row["database_name"]
        schema_name = row["schema_name"]
        tags.append(database_name +"."+schema_name+"."+tag_name)
    return tags

@st.cache_data
def get_schemas():
    get_schemas_sql = "show schemas in account"
    rows = session.sql(get_schemas_sql).collect()
    schemas = []
    for row in rows:
        schema_name = row["name"]
        database_name = row["database_name"]
        schemas.append(database_name +"."+schema_name)
    return schemas

def is_input_valid(profile_name,tag_maps,max_days):
    if(max_days.isnumeric() and int(max_days) < 1):
        st.error("Max days for reclassification should be > than 1")
        return False
    if(not (profile_name and profile_name.strip())):
        st.error("Profile name is empty")
        return False
    tag_category_map = {}
    for tag_map in tag_maps:
        if('tag_value' not in tag_map.keys()):
            continue
        tag_value = tag_map['tag_value']
        tag_name = tag_map['tag_name']
        semantic_categories = tag_map['semantic_categories']
        if(not (tag_value and tag_value.strip())):
            st.error("No tag value specified for " + tag_name)
            return False
        if(len(semantic_categories) == 0):
            st.error("No semantic category specified for " + tag_name)
            return False
        for category in semantic_categories:
            if (tag_name,category) in tag_category_map and tag_category_map[(tag_name,category)] != tag_value:
                st.error("Multiple values provided for " + tag_name+","+category+" pair" )
                return False
            else:
                tag_category_map[(tag_name,category)] = tag_value

    return True
                

    
# Write directly to the app
st.set_page_config(layout="wide")
st.title("Auto classification profile")
st.write(
    """
    This app enables to author the classification profile.
    """
)

# Get the current credentials
session = get_active_session()

categories = ['ADMINISTRATIVE_AREA_1'
, 'ADMINISTRATIVE_AREA_2'
, 'AGE'
, 'BANK_ACCOUNT'
, 'CITY'
, 'COUNTRY'
, 'DATE_OF_BIRTH'
, 'DRIVERS_LICENSE'
, 'EMAIL'
, 'ETHNICITY'
, 'GENDER'
, 'IMEI'
, 'IP_ADDRESS'
, 'LATITUDE'
, 'LAT_LONG'
, 'LONGITUDE'
, 'MARITAL_STATUS'
, 'MEDICARE_NUMBER'
, 'NAME'
, 'NATIONAL_IDENTIFIER'
, 'OCCUPATION'
, 'ORGANIZATION_IDENTIFIER'
, 'PASSPORT'
, 'PAYMENT_CARD'
, 'PHONE_NUMBER'
, 'POSTAL_CODE'
, 'SALARY'
, 'STREET_ADDRESS'
, 'TAX_IDENTIFIER'
, 'URL'
, 'VIN'
, 'YEAR_OF_BIRTH'
]
tags = get_tags()
schemas = get_schemas()


tag_maps = []
profile_obj = {}
with st.container():
    profile_schema = st.selectbox(label="Select schema where you want to store profile",options=schemas)
    col1,col2 = st.columns(2)
    profile_name = col1.text_input(label='Profile Name:red[(Required)]')
    replace_if_exists = col2.checkbox(label='Replace profile if already exists')
    min_days = st.number_input('Minimum object age:red[(Required)]',0,help='Number of days before running classification on a newly created object (minimum is 0)')
    max_days = st.text_input('Maximum number of days to wait before re-classifying object(Optional)',help='Number of days after which the object would be eligible for  re-classification (minimum is 1)')
    auto_tag = st.checkbox(label="Enable Auto tag",value=True,help='Automatically apply system and custom tags if checked, else only classification results are stored')
    st.divider()

    with st.expander(label='',expanded=True):

        num_tags = st.number_input("Number of tags to map (Optional)**(max 10)**",0,10)
        for i in range(0,num_tags):
            tag_map = {}
            col1,col2,col3,col4 = st.columns(4)
            map_to_semantic_category = col4.checkbox(key="col4"+str(i),label="Use snowflake semantic category value",help='Selecting this option means the tag will use Snowflake semantic categories as value')
            tag_name = col1.selectbox(key="col1"+str(i),
            label="Tag",
            options=tags)
            if not map_to_semantic_category:
                semantic_categories = col3.multiselect(
                key="col3"+str(i),
                label="Semantic categories",
                options=categories,
                help='Select the semantic categories which will be mapped to the value')
                default_value = ""
                tag_value = col2.text_input(key="col2"+str(i),label="Value",value=default_value,help = 'Value to be mapped to the semantic categories for this tag')
                tag_map['tag_value'] = tag_value
                tag_map['semantic_categories'] = semantic_categories
        
            tag_map['tag_name'] = tag_name
        
            tag_maps.append(tag_map)
    st.divider()
    schemas_to_attach = st.multiselect("Select schemas to attach profile to (Optional)",options=schemas)
    label = "Create and attach profile"
    if(len(schemas_to_attach) == 0):
        label = "Create profile"
    submitted = st.button(label=label)
    if submitted:
        if(is_input_valid(profile_name,tag_maps,max_days)):
            column_tag_map = {}
            column_tag_map['column_tag_map'] = tag_maps
            if(len(tag_maps) > 0):
                profile_obj['tag_map'] = column_tag_map
   
            profile_obj['minimum_object_age_for_classification_days'] = min_days
            if(max_days.isnumeric()):
                profile_obj['maximum_classification_validity_days'] = int(max_days)
            profile_obj['auto_tag'] = auto_tag
            profile_name = profile_schema+"."+profile_name
            create_profile_sql_comment = "/* This command will create the classification profile in the schema selected in the session */"
            replace_keyword = "or replace " if replace_if_exists else ""
            create_profile_sql = "create "+replace_keyword+" snowflake.data_privacy.classification_profile {}({});".format(profile_name,profile_obj)

            attach_profile_sql_comment = "/* Uncomment the following line and specify a schema name to attach this profile object to the schema */"
            
            if(len(schemas_to_attach) > 0):
                st.write("Attaching profile")
                attach_profile_sql = "alter schema {} set classification_profile='{}';"
                sqls_to_execute = [attach_profile_sql.format(x,profile_name) for x in schemas_to_attach]
                session.sql(create_profile_sql_comment +"\n\n"+create_profile_sql).collect()
                for sql in sqls_to_execute:
                    session.sql(sql).collect()
        
            else:
                session.sql(create_profile_sql_comment +"\n\n"+create_profile_sql).collect()
            rows = session.sql("select {}!describe()".format(profile_name)).collect()
            st.write("Profile description")
            st.json(rows[0][0])







