
from pendulum import datetime

from airflow.operators.empty import EmptyOperator
from datetime import datetime as dt

from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
    },
    tags=["etl", "super_smash_bros"],
) 

def super_smash_bros_operations_dag():

    games = ["Melee", "Brawl", "Ultimate"]

    for game in games:
        with TaskGroup(f"{game}_tournament_management") as tournament_management:
            venue_booking = EmptyOperator(task_id=f"{game}_venue_booking")
            sponsorship_acquisition = EmptyOperator(task_id=f"{game}_sponsorship_acquisition")
            player_registration = EmptyOperator(task_id=f"{game}_player_registration")
            prize_pool_allocation = EmptyOperator(task_id=f"{game}_prize_pool_allocation")

            # Tournament Management dependencies
            venue_booking.set_downstream(sponsorship_acquisition)
            venue_booking.set_downstream(player_registration)
            prize_pool_allocation.set_upstream(sponsorship_acquisition)
            prize_pool_allocation.set_upstream(sponsorship_acquisition)

        with TaskGroup(f"{game}_content_creation") as content_creation:
            gameplay_recording = EmptyOperator(task_id=f"{game}_gameplay_recording")
            commentary_recording = EmptyOperator(task_id=f"{game}_commentary_recording")
            editing_and_post_production = EmptyOperator(task_id=f"{game}_editing_and_post_production")
            streaming = EmptyOperator(task_id=f"{game}_streaming")
            live_event_coverage = EmptyOperator(task_id=f"{game}_live_event_coverage")
            social_media_interaction = EmptyOperator(task_id=f"{game}_social_media_interaction")

            # Content Creation dependencies
            gameplay_recording.set_downstream(streaming)
            gameplay_recording.set_downstream(live_event_coverage)
            gameplay_recording.set_downstream(social_media_interaction)
            commentary_recording.set_downstream(streaming)
            commentary_recording.set_downstream(live_event_coverage)
            commentary_recording.set_downstream(social_media_interaction)
            editing_and_post_production.set_upstream(streaming)
            editing_and_post_production.set_upstream(live_event_coverage)
            editing_and_post_production.set_upstream(social_media_interaction)

        with TaskGroup(f"{game}_player_development") as player_development:
            training_programs = EmptyOperator(task_id=f"{game}_training_programs")
            coach_assignments = EmptyOperator(task_id=f"{game}_coach_assignments")
            skill_assessment = EmptyOperator(task_id=f"{game}_skill_assessment")
            strategy_development = EmptyOperator(task_id=f"{game}_strategy_development")

            # Player Development dependencies
            coach_assignments.set_downstream(training_programs)
            coach_assignments.set_downstream(strategy_development)
            skill_assessment.set_upstream(training_programs)
            skill_assessment.set_upstream(strategy_development)

        with TaskGroup(f"{game}_community_engagement") as community_engagement:
            online_forums = EmptyOperator(task_id=f"{game}_online_forums")
            local_meetups = EmptyOperator(task_id=f"{game}_local_meetups")
            moderation = EmptyOperator(task_id=f"{game}_moderation")
            event_planning = EmptyOperator(task_id=f"{game}_event_planning")

            # Community Engagement dependencies
            event_planning.set_downstream(online_forums)
            online_forums.set_downstream(moderation)
            online_forums.set_downstream(local_meetups)

        # Ad Hoc Tasks
        technology_support = EmptyOperator(task_id=f"{game}_technology_support")
        marketing_and_promotion = EmptyOperator(task_id=f"{game}_marketing_and_promotion")
        analytics_and_reporting = EmptyOperator(task_id=f"{game}_analytics_and_reporting")

        start = EmptyOperator(task_id=f"{game}_start")
        end = EmptyOperator(task_id=f"{game}_end")

        start >> tournament_management >> technology_support >> end
        start >> content_creation >> end
        start >> player_development >> end
        start >> community_engagement >> end
        start >> marketing_and_promotion >> end
        start >> analytics_and_reporting >> end

super_smash_bros_operations_dag()