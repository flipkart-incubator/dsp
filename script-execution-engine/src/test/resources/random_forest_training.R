require(caret)
require(ggplot2)
library(pmml)
require(randomForest)

# Variables to store input csv locations
training_csv
testing_csv

trainingData <- read.csv(training_csv)
testData <- read.csv(testing_csv)

trainingData$fsn <- as.factor(trainingData$fsn)
trainingData$day_name <- as.factor(trainingData$day_name)

formula <- qty ~ asp + lp + ep + min_ep + exchangeDiscount + debitCard_per + creditCard_per + netBanking_per + emi + Freebie + basketDiscount + productDiscount + eventDayBAU + eventDayBBD + eventDayBED + eventDayBSD + eventDayDhanterasSale + eventDayMiniSalesEvnt + eventDayRepDaySales + eventDayTEST + eventClassDOTD + eventClassNone + eventClassSAVE_MORE + eventClassTOP_OFFER + offerTypeBank + offerTypeExchange + offerTypeFreebie + offerTypeProductDiscount + oosper + price_change_flag + price_change_flag_increase + no_of_days_lastprchange + no_of_days_lastprchange_increase + min_ep_1_day + min_ep_7_day + ep_1_day + lp_1_day + asp_1_day + lowest_price_ever + discount_asp_lp + discount_min_ep + discount_min_ep_7_day + discount_lowest_price_ever + discount_lp + discount_ep + price_drop_last_day + price_drop_last7_day + visibility + visibility_7_day + discount_min_ep_lag + discount_min_ep_lag7 + sale_1_days_avg + sale_7_days_med + sale_7_days_avg + sale_7_days_sd + sale_3_days_med + PLC

start.time <- Sys.time()
rf_model<-randomForest(formula,data=trainingData, ntree = 10, nodesize = 7)
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken


# Create predictions in this script as well so that it can be compared with the predictions of the next script.
op <- predict(rf_model, testData)
comparision_csv_location <- "output_first.csv"
write.csv(data.frame(op), comparision_csv_location)
