with critic_reviews as (
    select
        critic,
        count(*) as number_of_critics
    from `PUT YOUR GCP PROJECT ID HERE.rotten_tomatoes_reviews.critic_reviews`
    where original_score is not null
    group by critic
    order by number_of_critics desc
)


SELECT *
FROM critic_reviews
