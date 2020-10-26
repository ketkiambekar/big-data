In This example, we write a simple Mapreduce program. 
Suppose we have some cards on hand (<52) from a playing cards deck, we find the cards missing from among cards on hand. 
We count the cards using their suits and ranks. We output the suits and ranks of the missing cards. 
The mapper counts the cards with suits as the keys and ranks as the values. 
The reducer outputs the missing values (ranks) for each key (suits)  
