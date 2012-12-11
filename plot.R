#!/usr/bin/Rscript
dd <- read.table('/tmp/to.csv', header=T, sep=",")

max_y <- max(dd)

plot_colors<-c("red", "blue", "forestgreen")

png(filename="/tmp/plot.png", bg="white")

plot(dd$timeouts, type="o", col=plot_colors[1], ylim=c(0,max_y), axes=FALSE, ann=FALSE)
axis(1, las=1, at=0:23)
axis(2, las=1, at=50*0:max_y)
box()
lines(dd$deadlocks, type="o", pch=22, lty=2, col=plot_colors[2])
title(main="timeouts vs Lock Waits")
title(xlab="Heure")
legend(1, max_y, c("timeouts", "Lock Waits"), cex=0.8, col=plot_colors, pch=21:23, lty=1:3)
dev.off()
