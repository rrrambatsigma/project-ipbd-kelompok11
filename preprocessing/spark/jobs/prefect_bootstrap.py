from loguru import logger
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule

WORK_POOL_NAME = "preprocessing-pool"


def register():
    from prefect.flows import load_flow_from_script

    flow = load_flow_from_script(
        "/app/jobs/preprocessing_flow.py", "Euro News Preprocessing"
    )

    deployment = Deployment.build_from_flow(
        flow=flow,
        name="euro-news-preprocessing",
        work_pool_name=WORK_POOL_NAME,
        schedule=(CronSchedule(cron="0 16 * * 1-5", timezone="Asia/Jakarta")),
        tags=["euro-news", "preprocessing", "sentiment"],
    )
    deployment.apply()
    logger.info("Deployment registered via build_from_flow")


def start_worker():
    import asyncio
    from prefect.workers.process import ProcessWorker

    async def _run():
        worker = ProcessWorker(work_pool_name=WORK_POOL_NAME)
        await worker.start()

    asyncio.run(_run())


if __name__ == "__main__":
    register()
    start_worker()
