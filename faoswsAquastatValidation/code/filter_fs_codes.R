
# unfiltered data
df <- d

# parameter domain code
if (swsContext.computationParams == 'Crops')
  dc <- 'QC'
if (swsContext.computationParams == 'Livestock Primary')
  dc <- 'QL'







dc <- 'QC'

df <- fread("//hqlprsws1.hq.un.fao.org/sws_r_share/QualityIndicators/mapping_tables/testdata.csv")
el_df <- fread('~/quality_backend/quality_indicator_report/old/domain_element.csv', stringsAsFactors = FALSE)

dc <- 'QL'


filter_fs_codes <-
  function(df, dc) {

    require(data.table)
    require(stringr)

    if (is.null(dc))
      stop('User needs to set the domain code')

    # read in mapping tables
    mtbli <- fread("//hqlprsws1.hq.un.fao.org/sws_r_share/QualityIndicators/mapping_tables/domain_item.csv")
    mtble <- fread("//hqlprsws1.hq.un.fao.org/sws_r_share/QualityIndicators/mapping_tables/domain_element.csv")


    # Faostat list of Items (includes: Items)
    fs_items <- mtbli[domain_code %in% dc, .(domain_code, var_code, var_code_sws)]
    fs_item_list <- unique(fs_items$var_code_sws)

    # Get element code (only non-analytical variables)
    fs_element <- mtble[domain_code %in% dc, .(domain_code, element_code)]
    fs_element_list <- unique(fs_element$element_code)


    # Get columns containing item and element in the name
    item_column <- names(df)[str_detect(names(df), 'Item|item')]
    element_column <- names(df)[str_detect(names(df), 'Element|elem')]

    # Change the names of the item and element code columns
    if (!is.null(item_column))
      names(df)[str_detect(names(df), 'Item|item')] <- 'ItemCode'

    if (!is.null(element_column))
      names(df)[str_detect(names(df), 'Elem|Elem')] <- 'ElementCode'

    # Filtering the data frame
    if ("ItemCode" %in% names(df))
      df1 <- df[ItemCode %in% fs_item_list]

    if ("ElementCode" %in% names(df))
      df1 <- df1[ElementCode %in% fs_element_list]

    return(df1)

  }

fidata <- filter_fs_codes(df = d, dc = 'QC')
