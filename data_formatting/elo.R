files = list.files(pattern="atp_matches_[^_]*.csv")

matches_raw= do.call("rbind", lapply(files, function(x) read.csv(x, sep=",")))
matches <- matches_raw[c("winner_name","loser_name","tourney_level","tourney_date","match_num")]
firstDate <- as.Date("1900-01-01")
matches$tourney_date <- as.Date(as.character(matches$tourney_date),format='%Y%m%d', origin = "1900/01/01")

matches <- matches[order(matches$tourney_date,matches$match_num),]
playersToElo <- new.env(hash=TRUE)
matchesCount <- new.env(hash=TRUE)


# Run computeElo for elo results in an environment indexed by player names
computeElo <- function() {
    apply(matches,1,updateMatchesCountByRow)
    apply(matches,1,computeEloByRow)
     
    return(playersToElo)
}

# Gives the highest elo ratings
summaryPlayers <- function() {
    playersToMax <- data.frame(ranking=1500,meanr=1500,medianr=1500,name="Nobody")
    for (pl in ls(playersToElo)) {
        player <- playersToElo[[pl]]
        ## player <- player[order(player$date,player$num,decreasing=TRUE),]
        ## player <- player[!duplicated(player$date),]
        ## player <- player[order(player$date,player$num,decreasing=FALSE),]

        newRow <- data.frame(ranking=max(player$ranking),meanr=mean(player$ranking),medianr=median(player$ranking),name=pl)
        playersToMax <- rbind(playersToMax,newRow)
    }

    playersToMax <- playersToMax[order(playersToMax$ranking,decreasing=TRUE),]
    return(playersToMax)
}

### Peaks

getYear <- function(year) {
    return(as.Date(paste(year,"-01-01",sep="")))
}

getYearMonth <- function(year,month) {
    return(as.Date(paste(year,"-",month,"-01-",sep="")))
}

betweenDates <- function(date1,date2) {
    playersToMax <- data.frame(ranking=1500,meanr=1500,medianr=1500,name="Nobody")
    for (pl in ls(playersToElo)) {
        player <- playersToElo[[pl]]
        ## player <- player[order(player$date,player$num,decreasing=TRUE),]
        ## player <- player[!duplicated(player$date),]
        ## player <- player[order(player$date,player$num,decreasing=FALSE),]
        player <- player[which(player$date>=date1),]
        player <- player[which(player$date <= date2),]
        newRow <- data.frame(ranking=max(player$ranking),meanr=mean(player$ranking),medianr=median(player$ranking),name=pl)
        playersToMax <- rbind(playersToMax,newRow)
    }
    playersToMax <- playersToMax[order(playersToMax$ranking,decreasing=TRUE),]
    return(playersToMax)
}

peakRafa <- betweenDates(getYearMonth(2013,"09"),getYearMonth(2013,"10"))
peakRafa2 <- betweenDates(getYearMonth(2009,"05"),getYearMonth(2009,"06"))
peakRafa3 <- betweenDates(getYearMonth(2010,"05"),getYearMonth(2010,"06"))
peakBorg <- betweenDates(getYearMonth(1980,"08"),getYearMonth(1980,"09"))
peakMcenroe <- betweenDates(getYearMonth(1985,"04"),getYearMonth(1985,"05"))
peakRoger <- betweenDates(getYearMonth(2007,"02"),getYearMonth(2007,"04"))
peakNovak <- betweenDates(getYearMonth(2015,"05"),getYearMonth(2015,"06"))
peakPete <- betweenDates(getYearMonth(1994,"05"),getYearMonth(1994,"07"))
peakLendl <- betweenDates(getYearMonth(1988,"01"),getYearMonth(1988,"03"))
peakLendl2 <- betweenDates(getYearMonth(1986,"03"),getYearMonth(1986,"04"))
peakLaver <- betweenDates(getYearMonth(1976,"02"),getYearMonth(1976,"03"))


### Elo computation details

computeEloByRow <- function(row) {
    updateElo(playersToElo, row[1], row[2], row[1], row[3],row[4],row[5])
    return(0)
}

updateMatchesCountByRow <- function(row) {
    updateMatchesCount(row[1],row[2])
    return(0)
}

updateMatchesCount <- function (playerA, playerB) {
    if(is.null(matchesCount[[playerA]])) { matchesCount[[playerA]] <- 0 }
    if(is.null(matchesCount[[playerB]])) { matchesCount[[playerB]] <- 0 }
    matchesCount[[playerA]] <- matchesCount[[playerA]]+1
    matchesCount[[playerB]] <- matchesCount[[playerB]]+1
}

updateElo <- function (plToElo, playerA, playerB, winner, level, matchDate,matchNum) {
    rA <- tail(plToElo[[playerA]]$ranking,n=1)
    rB <- tail(plToElo[[playerB]]$ranking,n=1)

    if(is.null(rA)) {
        plToElo[[playerA]] <- data.frame(ranking=1500, date=firstDate, num=0)
        rA <- 1500
    }
    if(is.null(rB)) {
        plToElo[[playerB]] <- data.frame(ranking=1500, date=firstDate, num=0)
        rB <- 1500
    }
        
    eA <- 1 / (1 + 10 ^ ((rB - rA)/400))
    eB <- 1 / (1 + 10 ^ ((rA - rB)/400))
    
    if (winner==playerA) {
        sA <- 1
        sB <- 0
    } else {
        sA <- 0
        sB <- 1
    }

    kA <- 250/((matchesCount[[playerA]]+5)^0.4)
    kB <- 250/((matchesCount[[playerB]]+5)^0.4)
    k <- ifelse(level == "G", 1.1, 1)

    rA_new <- rA + (k*kA) * (sA-eA)
    rB_new <- rB + (k*kB) * (sB-eB)

    plToElo[[playerA]] <- rbind(plToElo[[playerA]],data.frame(ranking=rA_new, date=matchDate, num=matchNum))
    plToElo[[playerB]] <- rbind(plToElo[[playerB]],data.frame(ranking=rB_new, date=matchDate, num=matchNum))
}



## Some Plotting 

greaterEqualYear <- function(pl, year) {
    return(pl[pl$date>=getYear(year),])
}

plotGuys <- function() {
    startYear <- 2007
    novak <- greaterEqualYear(playersToElo[["Novak Djokovic"]],startYear)
    roger <- greaterEqualYear(playersToElo[["Roger Federer"]],startYear)
    rafa <- greaterEqualYear(playersToElo[["Rafael Nadal"]],startYear)
    andi <- greaterEqualYear(playersToElo[["Andy Murray"]],startYear)
    david <- greaterEqualYear(playersToElo[["David Ferrer"]],startYear)
    delpo <- greaterEqualYear(playersToElo[["Juan Martin Del Potro"]],startYear)
    minDate <- min(roger$date[roger$date!=min(roger$date)])
    maxDate <- max(roger$date)
    plot(roger$date,roger$ranking,type="l",xlab="Date",ylab="Points",main="Elo",xlim=c(minDate,maxDate),ylim=c(2000,2330),col="red",lty=1)
    lines(rafa$date,rafa$ranking,type="l",col="blue",lty=2)
    lines(novak$date,novak$ranking,type="l",col="black",lty=4)
    lines(andi$date,andi$ranking,type="l",col="purple",lty=6)
    lines(david$date,david$ranking,type="l",col="azure4",lty=2)
    lines(delpo$date,delpo$ranking,type="l",col="green",lty=3)
}

plotOldies <- function() {
    startYear <- 1980
    john <- greaterEqualYear(playersToElo[["John Mcenroe"]],startYear)
    jimmy <- greaterEqualYear(playersToElo[["Jimmy Connors"]],startYear)
    ivan <- greaterEqualYear(playersToElo[["Ivan Lendl"]],startYear)
    bjorn <- greaterEqualYear(playersToElo[["Bjorn Borg"]],startYear)
    mats <- greaterEqualYear(playersToElo[["Mats Wilander"]],startYear)
    stefan <- greaterEqualYear(playersToElo[["Stefan Edberg"]],startYear)
    minDate <- as.Date("1980-01-01")
    maxDate <- as.Date("1990-01-01")
    plot(jimmy$date,jimmy$ranking,type="l",xlab="Date",ylab="Points",main="Elo",xlim=c(minDate,maxDate),ylim=c(2000,2330),col="red",lty=1)
    lines(ivan$date,ivan$ranking,type="l",col="blue",lty=2)
    lines(john$date,john$ranking,type="l",col="black",lty=4)
    lines(bjorn$date,bjorn$ranking,type="l",col="purple",lty=6)
    lines(mats$date,mats$ranking,type="l",col="azure4",lty=2)
    lines(stefan$date,stefan$ranking,type="l",col="green",lty=3)
}