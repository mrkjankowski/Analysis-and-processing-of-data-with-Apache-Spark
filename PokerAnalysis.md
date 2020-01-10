

# **Code:**            


    from pyspark import SparkContext
    from operator import add
             
    #-----------------Functions--------------------#     
    
    def FigureCounting(Data): 
            #Figure[0] = Nothing
            #Figure[1] = OnePair
            #Figure[2] = DoublePair
            #Figure[3] = Three
            #Figure[4] = Straight
            #Figure[5] = Flush
            #Figure[6] = Full
            #Figure[7] = Four
            #Figure[8] = StraightFlush
            #Figure[9] = RoyalFlush
            C_Figure = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            figure = Data
            if figure == '0':
                C_Figure[0] += 1
            elif figure == '1':
                C_Figure[1] += 1
            elif figure == '2':
                C_Figure[2] += 1
            elif figure == '3':
                C_Figure[3] += 1
            elif figure == '4':
                C_Figure[4] += 1
            elif figure == '5':
                C_Figure[5] += 1
            elif figure == '6':
                C_Figure[6] += 1
            elif figure == '7':
                C_Figure[7] += 1
            elif figure == '8':
                C_Figure[8] += 1
            elif figure == '9':
                C_Figure[9] += 1
            return C_Figure
    
    def Results(Prob): #prints results 
        r = 7
        print("\n")
        print("Chance to receive a specific poker figure: \n")
        print("Nothing in Hand = " + str(round(Prob[0],r)) + "%")
        print("One Pair = " + str(round(Prob[1],r)) + "%")
        print("Double Pair = " + str(round(Prob[2],r)) + "%")
        print("Three of a kind = " + str(round(Prob[3],r)) + "%" )
        print("Straight = " +str( round(Prob[4],r)) + "%")
        print("Flush = " + str(round(Prob[5],r)) + "%")
        print("Full house = " + str(round(Prob[6],r)) + "%")
        print("Four of a kind = " + str(round(Prob[7],r)) + "%")
        print("Straight Flush= " + str(round(Prob[8],r)) + "%")
        print("Royal Flush = " + str(round(Prob[9],r)) + "%")
        
    def div(self,other):
        return self/other
        
    def main():
        Data = File.map(lambda line: line.split(";")) \
            .map(lambda x: x[len(x)-1]) \
            .map(lambda x: FigureCounting(x)) \
            .reduce(lambda x,y:[x[0] + y[0],x[1] + y[1],x[2] + y[2],x[3] + y[3],x[4] + 				y[4],x[5] + y[5],x[6] + y[6],x[7] + y[7],x[8] + y[8],x[9] + y[9]])
        Data1 = sc.parallelize(Data)
        adding = Data1.reduce(add) 
        print("Number of total data: {}".format(adding))
        print("Number of specific classes in data: {}".format(Data))
        ProbabilityOfClass = Data1.map(lambda x: div(x,adding)*100) #returns Data1[i]/ sum of Data
        Results(ProbabilityOfClass.collect())
    #-----------------End of Functions--------------------#  
    
    sc = SparkContext("spark://ip-172-31-46-39.eu-west-1.compute.internal:7077", "PokerApp")
    File = sc.textFile("PokerData.csv")
    main()
    sc.stop()


 
# **Results:**



```
Number of total data: 1000000
Number of specific classes in data: [501209, 422498, 47622, 21121, 3885, 1996, 1424, 230, 12, 3]


Chance to receive a specific poker set: 

Nothing in Hand = 50.1209%
One Pair = 42.2498%
Double Pair = 4.7622%
Three of a kind = 2.1121%
Straight = 0.3885%
Flush = 0.1996%
Full house = 0.1424%
Four of a kind = 0.023%
Straight Flush= 0.0012%
Royal Flush = 0.0003%
```

