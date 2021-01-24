require(caret)
require(ggplot2)
library(pmml)
require(randomForest)

op <- predict(model, data)
output_csv_location <- "output_second.csv"
write.csv(data.frame(op), output_csv_location)
