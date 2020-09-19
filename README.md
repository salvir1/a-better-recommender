# A better recommender
## A head-to-head comparision of two recommendation engines
Two different approaches to recommendation engines were compared in this project. Both models were built to make user-specific recommendations, but their approaches were very different. Simulated output from these two models was fed into a multi-armed bandit model to allow for a direct comparison of the two approaches.

One model took in user ratings data and created a ALS matrix factorization. ALS models and other ratings-based models do not allow for the incorporation of item features because their addition would make them too computationally complex. The second approach--a Learn to Rank model--simplifies the ratings data into boolean values (usually if an item has been ranked by a user then it is ranked 1, and -1 otherwise) to allow for the incorporation of item features. 

While the loss functions that drive these two models are very different, if given a list of unseen items to rank, both generate relative rankings for these items. Which of these models generates a better relative ranking when compared to a ground truth? In this project, a multi-armed bandit model was employed to compare the model results.
## What to recommend?
Movie information was chosen for this project since movie ratings and features data are readily available--although 'readily' is a relative term. Working with large data presents its unique challenges whether the data is readily available or not. Movielens.org provides lists of movie ratings by their site's users. Lists range from small school project sizes of __ by __ to more complete ___ by .

In order to be as comprehensive as possible, the larger dataset was chosen. 
## Process
The first step was to look at the distribution of ratings by movie. The recommendation engines were going to be judged on how well their 'suggestions' compared with a user's top rated movies. Was it necessary to include all movies in the models? Or could some be filtered out because virtually no one rated them highly, thus their chances of making it into either model's top ranking was nonexistent?

This step is basically using a simple item feature only recommendation model as a first pass model. (As an aside, it's one way to build item feature details into a tiered model approach in which one layer relies on a ratings-only ALS model. Can other tiers be added to improve an ALS model even more?)

