setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../../scripts/h2o-r-test-setup.R")
library(MASS)
library(hglm)
##
# Copy example from HGLM package and compare results from our implementation.
##

test.HGLMData1 <- function() {
  browser()
  data(semiconductor)
  h2odata <- as.h2o.data.frame(semiconductor)
  h2odata$Device <- h2o.asfactor(h2odata$Device)
  yresp <- "y"
  xlist <- c("x1", "x3", "x5", "x6")
  z <- c(1)
 # z<-c("Device")
  m11H2O <- h2o.glm(x=xlist, y=yresp, family="gaussian", rand_family = c("gaussian"), rand_link=c("identity"), 
                    training_frame=h2odata, HGLM=TRUE, random_columns=z)
  # m11 <- hglm(fixed = y ~ x1 + x3 + x5 + x6,
  #             random = ~ 1|Device,
  #             family = Gamma(link = log),
  #             disp = ~ x2 + x3, data = semiconductor)
  m11 <- hglm(fixed = y ~ x1 + x3 + x5 + x6,
              random = ~ 1|Device,
              family=gaussian(link="identity"), 
              rand.family=gaussian(link="identity"),data = semiconductor)
  summary(m11)
  plot(m11, cex = .6, pch = 1,
       cex.axis = 1/.6, cex.lab = 1/.6,
       cex.main = 1/.6, mar = c(3, 4.5, 0, 1.5))
 }

doTest("Comparison of H2O to R with HGLM 1", test.HGLMData1)


