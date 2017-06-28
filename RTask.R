# --------------
# Author: J. M. B. Josko
# --------------
# Preamble - Loading required packages and set current working directory
# --------------

library(DBI)
library(RMySQL)  
library(magrittr)
library(tibble)
library(dplyr)
library(plyr)

setwd("D:/Luizalabs")
fname = paste(getwd(), "stream.csv", sep="/")

# --------------
# Step 1 - Get all required data
# --------------

# Step 1.1 - Stablish MySQL connection
conMy = dbConnect(MySQL(),user="desafio", password ="1i450U", db="bigdata_desafio", host="big-data-analytics-test.cd2vfjltihkr.us-east-1.rds.amazonaws.com")

# Step 1.2 - Get PRODUCTS, ORDER and ORDERITEM and leave MySQL, as follows
wk_prod = dbGetQuery(conMy, "SELECT * from products")
wk_Item = dbGetQuery(conMy, "SELECT * FROM orderitem")
wk_ord = dbGetQuery(conMy, "SELECT date(order_date) as date, order_id, city, state, status, total, shipping_price FROM orders")
dbDisconnect(conMy)  

# --------------
# Step 2 - Build Dimensions
# --------------

# Step 2.1 - Build Brand Dimension
wk_prod = transform(wk_prod, brand_id=as.numeric(as.factor(brand)))   # Step 2.1.1 - Define unique keys 
dim_brand = unique(wk_prod[, c("brand_id", "brand")])           # Step 2.1.2 - Create Brand Dimension
dim_brand = rbind(dim_brand, c(brand_id=-1, brand="Not Applicable"))

# Step 2.2 - Build Category/SubCategory Dimension
wk_prod = transform(wk_prod, sub_category_id=as.numeric(as.factor(paste(category,sub_category)))) # Step 2.2.1 - Define unique keys 
dim_scateg = unique(wk_prod[, c("sub_category_id", "sub_category", "category")])            # Step 2.2.2 - Create Sub_cat Dimension
dim_scateg = rbind(dim_scateg, c(sub_category_id=-1, sub_category="Not Applicable", category="Not Applicable"))

# Step 2.3 - Build Status Order Dimension
wk_ord = transform(wk_ord, status_id=as.numeric(as.factor(status)))   # Step 2.3.1 - Define unique keys 
dim_status = unique(wk_ord[, c("status_id", "status")])               # Step 2.3.2 - Create Status Order Dimension
dim_status = rbind(dim_status, c(status_id=-1, status="Not Applicable"))

# Step 2.4 - Build Date Dimension
dim_date = unique(wk_ord["date"])

# Step 2.5 - Build City/State Dimension
wk_ord = transform(wk_ord, city_id=as.numeric(as.factor(paste(city,state))))        # Step 2.5.1 - Define unique keys 
dim_city = unique(wk_ord[, c("city_id", "city", "state")])                          # Step 2.5.2 - Create City/State Dimension
dim_city = rbind(dim_city, c(city_id=-1, city="Not Applicable", state="Not Applicable"))

# --------------
# Step 3 - Build Fact Relation
# --------------

# Step 3.1 - Join Order + OrderItem
fact_ord = join(wk_ord[c("date","order_id","shipping_price","total","city_id","status_id")], 
                wk_Item[c("order_id","product_id", "selling_price")], by="order_id", type = "inner")

# Step 3.2 - Join Fact Relation + Product
fact_ord = join(fact_ord, wk_prod[c("brand_id","sub_category_id","product_id")], by="product_id", type = "inner")

#remove (wk_prod, wk_ord)

# Step 3.3 - Summarize
fact_ord = fact_ord %>%  group_by(date, city_id, status_id, brand_id, sub_category_id) %>%
  dplyr::summarize(total = sum(total), selling_price = sum(selling_price), shipping_price = sum(shipping_price),
            countOrder = length(order_id[status_id==2])) 

# Step 3.4 - Set event_type not applicable
fact_ord$event_type_id <- rep(-1,nrow(fact_ord))
fact_ord$countTypeEvent <- rep(0,nrow(fact_ord))

# --------------
# Step 4 - Arrange stream data
# --------------

if (file.exists(fname))
{
  fstr <- read.csv(file=fname, header=TRUE, sep=",")   # Step 4.1.1 Load raw strem data
  wk_str = fstr %>%  group_by(datetime, event_type) %>% dplyr::summarize(countTypeEvent = length(datetime))
  wk_str = transform(wk_str, event_type_id=as.numeric(as.factor(event_type)))  
  
  # build Event Type Dimension
  dim_event = unique(wk_str[, c("event_type_id", "event_type")])  
  dim_event$event_type <- as.character(dim_event$event_type)
  dim_event = rbind(dim_event, c(event_type_id=-1, event_type="Not Applicable"))
  
  # Arrange columns Names
  wk_str = rename(wk_str,c('datetime'='date'))
  cols1 = c("city_id", "status_id", "brand_id", "sub_category_id")
  wk_str = cbind(wk_str, setNames( lapply(cols1, function(x) x=-1), cols1) )
  cols2 = c("total", "selling_price", "shipping_price", "countOrder")
  wk_str = cbind(wk_str, setNames( lapply(cols2, function(x) x=0), cols2) )
  
  # Gather order and Stream data 
  fact_ord = as.data.frame(fact_ord)
  fact_ord = join(fact_ord, wk_str[,-2], type="full")
  
}

# --------------
# Step 5 - Generate Dimensions and Facy Relation in csv files
# --------------


write.csv(dim_brand, file = "dim_brand.csv",row.names=F, na="", quote = FALSE)
write.csv(dim_scateg, file = "dim_scateg.csv",row.names=F, na="", quote = FALSE)
write.csv(dim_status, file = "dim_status.csv",row.names=F, na="", quote = FALSE)
write.csv(dim_date, file = "dim_date.csv",row.names=F, na="", quote = FALSE)
write.csv(dim_city, file = "dim_city.csv",row.names=F, na="", quote = FALSE)
write.csv(dim_event, file = "dim_event.csv",row.names=F, na="", quote = FALSE)
write.csv(fact_ord, file = "fact_ord.csv",row.names=F, na="", quote = FALSE)

# --------------
# All done!
# --------------

