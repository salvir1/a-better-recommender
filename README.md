# A better recommender
## A head-to-head comparision of two recommendation engines
Two different approaches to recommendation engines were compared in this project. Both models were built to make user-specific recommendations, but their approaches were very different. Simulated output from these two models was fed into a multi-armed bandit model to allow for a direct comparison of the two approaches.

One model took in user ratings data and created a ALS matrix factorization. ALS models and other ratings-based models do not allow for the incorporation of item features because their addition would make them too computationally complex. The second approach--a Learn to Rank model--simplifies the ratings data into boolean values (usually if an item has been ranked by a user then it is ranked 1, and -1 otherwise) to allow for the incorporation of item features. 

While the loss functions that drive these two models are very different, if given a list of unseen items to rank, both generate relative rankings for these items. Which of these models generates a better relative ranking when compared to a ground truth? In this project, a multi-armed bandit model was employed to compare the model results.


