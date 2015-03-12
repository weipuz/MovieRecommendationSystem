# Movie Recommendation System
This is a MapReduce and HBase application for serveal features in a Movie Recommendation System.
It contains two main part:
- 1. Movie similiarity analyze based on user ratings
- 2. Movie and User dependency analyze based on movie's genres and user's gender, occupation and age group.

It also contains several utility functions to help process the raw data and analyze the results:
- 1. ETL codes：extract and transform the raw data such as UserInfo, MovieInfo, MovieRating, into HBase tables.
- 2. Client codes to quary and report the results in HBase tables, such as search and return the top 10 similar movies of a given movie;
- 3. Client codes to transform the output in HBase to csv files as report.

###The data: 
We use the follow datasets:

1. Movie Tweeting dataset. The dataset was collected by Simon Dooms4 by processing public tweets on Twitter and it contains 100,000 anonymous ratings of approximately 10,000 movies made by 16,000 users.  https://github.com/sidooms/MovieTweetings
2. Larger dataset from MovieLens: 1 million rating dataset from: http://files.grouplens.org/datasets/movielens/ml-1m.zip.

###Data Format:

movies.dat that stores movie ID, movie title and its genres in each line. This file uses the format ID::Title::Genres <br>e.g.:
```
0004936::The Bank (1915)::Comedy|Short
0004972::The Birth of a Nation (1915)::Drama|History|Romance|War
0005078::The Cheat (1915)::Drama
0006684::The Fireman (1916)::Short|Comedy
0006689::The Floorwalker (1916)::Short|Comedy
0007264::The Rink (1916)::Comedy|Short
```
ratings.dat. This file stores ratings in the format user_id::movie_id::rating::timestamp. These ratings are scaled from 0 to 10 and timestamp is measured in UNIX seconds since 1/1/1970 UTC. You can find some examples of rating.dat below:
```
1::1074638::7::1365029107
1::1853728::8::1366576639
2::0104257::8::1364690142
2::1259521::8::1364118447
2::1991245::7::1364117717
3::1300854::7::1368156300
```
users.dat. User information is in the file "users.dat" and is in the following format: UserID::Gender::Age::Occupation::Zip-code
```
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
4::M::45::7::02460
5::M::25::20::55455
```
- Gender is denoted by a "M" for male and "F" for female
- Age is chosen from the following ranges:

	*  1:  "Under 18"
	* 18:  "18-24"
	* 25:  "25-34"
	* 35:  "35-44"
	* 45:  "45-49"
	* 50:  "50-55"
	* 56:  "56+"

- Occupation is chosen from the following choices:

	*  0:  "other" or not specified
	*  1:  "academic/educator"
	*  2:  "artist"
	*  3:  "clerical/admin"
	*  4:  "college/grad student"
	*  5:  "customer service"
	*  6:  "doctor/health care"
	*  7:  "executive/managerial"
	*  8:  "farmer"
	*  9:  "homemaker"
	* 10:  "K-12 student"
	* 11:  "lawyer"
	* 12:  "programmer"
	* 13:  "retired"
	* 14:  "sales/marketing"
	* 15:  "scientist"
	* 16:  "self-employed"
	* 17:  "technician/engineer"
	* 18:  "tradesman/craftsman"
	* 19:  "unemployed"
	* 20:  "writer"
2. 
