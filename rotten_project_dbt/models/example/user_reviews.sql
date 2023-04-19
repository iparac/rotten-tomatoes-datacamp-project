with user_reviews as (
    select
        title,
        -- review,
        round(avg(score),2) as average_score
    from `quiet-maxim-379023.rotten_tomatoes_reviews.user_reviews`
    group by title
    order by average_score desc
)


SELECT *
FROM user_reviews
