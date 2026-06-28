CREATE OR REFRESH STREAMING TABLE ws_restaurant_dbxproject.`02_silver`.fact_reviews(
    CONSTRAINT valid_sentiment EXPECT(sentiment IN ('positive','neutral','negative')) ON VIOLATION DROP ROW,
    CONSTRAINT valid_rating EXPECT(rating>0) ON VIOLATION DROP ROW
)
AS
with model_response AS(
select *,
from_json(
    ai_query(
        'databricks-gpt-oss-20b',
        CONCAT(
            'Analyze the following review text and return ONLY a valid JSON object with this exact structure: ',
            '{"sentiment": "<positive/neutral/negative>", ',
            '"issue_delivery": <true/false>, ',
            '"issue_delivery_reason": "<reason or empty string>", ',
            '"issue_food_quality": <true/false>, ',
            '"issue_food_quality_reason": "<reason or empty string>", ',
            '"issue_pricing": <true/false>, ',
            '"issue_pricing_reason": "<reason or empty string>", ',
            '"issue_portion_size": <true/false>, ',
            '"issue_portion_size_reason": "<reason or empty string>"}. ',
            'Rules: sentiment must be exactly one of: positive, neutral, negative. ',
            'Each issue field is true/false only. ',
            'Each reason field should contain a brief explanation if the issue is true, otherwise empty string. ',
            'Review text: ', review_text
          )
    ),
    'STRUCT<sentiment:STRING, issue_delivery:BOOLEAN, issue_delivery_reason:STRING, issue_food_quality:BOOLEAN, issue_food_quality_reason:STRING, issue_pricing:BOOLEAN, issue_pricing_reason:STRING, issue_portion_size:BOOLEAN, issue_portion_size_reason:STRING>'
) AS analysis_json
from STREAM(ws_restaurant_dbxproject.`01_bronze`.reviews)
)
select
    review_timestamp,
    review_id,
    order_id,
    customer_id,
    restaurant_id,
    rating,
    review_text,
    analysis_json,
    analysis_json.sentiment as sentiment,
    analysis_json.issue_delivery as issue_delivery,
    analysis_json.issue_delivery_reason as issue_delivery_reason,
    analysis_json.issue_food_quality as issue_food_quality,
    analysis_json.issue_food_quality_reason as issue_food_quality_reason,
    analysis_json.issue_pricing as issue_pricing,
    analysis_json.issue_pricing_reason as issue_pricing_reason,
    analysis_json.issue_portion_size as issue_portion_size,
    analysis_json.issue_portion_size_reason as issue_portion_size_reason
from model_response;