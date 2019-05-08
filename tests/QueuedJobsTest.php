<?php

/**
 *
 *
 * @author Marcus Nyeholt <marcus@symbiote.com.au>
 */
class QueuedJobsTest extends SapphireTest {

    /**
     * We need the DB for this test
     *
     * @var bool
     */
    protected $usesDatabase = true;

	public function setUp() {
		parent::setUp();

		Config::nest();
		// Two restarts are allowed per job
		Config::inst()->update('QueuedJobService', 'stall_threshold', 2);
	}

	public function tearDown() {
		Config::unnest();
		parent::tearDown();
	}


	/**
	 * @return QueuedJobService
	 */
	protected function getService() {
		return singleton("TestQJService");
	}

	public function testQueueJob() {
		$svc = $this->getService();

		// lets create a new job and add it tio the queue
		$job = new TestQueuedJob();
		$jobId = $svc->queueJob($job);
		$list = $svc->getJobList();

		$this->assertEquals(1, $list->count());

		$myJob = null;
		foreach ($list as $job) {
			if ($job->Implementation == 'TestQueuedJob') {
				$myJob = $job;
				break;
			}
		}

		$this->assertNotNull($myJob);
		$this->assertTrue($jobId > 0);
		$this->assertEquals('TestQueuedJob', $myJob->Implementation);
		$this->assertNotNull($myJob->SavedJobData);
	}

    public function testJobRunAs()
    {
        $svc = $this->getService();
        $list = $svc->getJobList();
        foreach ($list as $job) {
            $job->delete();
        }

		// Create a new job with no run as ID it will have the current logged in user.
        $adminID = $this->logInWithPermission('ADMIN');
        $job = new TestQueuedJob();
        $job->JobTitle = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $job->Signature = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $jobId = $svc->queueJob($job);
        $list = $svc->getJobList();

        $myJob = $list->byID($jobId);
        $this->assertEquals("ADMIN@example.org", $myJob->RunAs()->Email);

        // Create a new job with no run as ID it will have the current logged in user.
        Member::currentUser()->logOut();
        $job = new TestQueuedJob();
        $job->JobTitle = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $job->Signature = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $jobId = $svc->queueJob($job);
        $list = $svc->getJobList();

        $myJob = $list->byID($jobId);
        $this->assertEquals(null, $myJob->RunAs()->Email);

        // Log in as another user but set the run ID the Run As User has
        $dummyID = $this->logInWithPermission('DUMMY');
        $job = new TestQueuedJob();
        $job->JobTitle = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $job->Signature = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $jobId = $svc->queueJob($job);

        $myJob = $list->byID($jobId);
        // RunAsID is set to current user in QueuedJobService:queueJob()
        $this->assertEquals("DUMMY@example.org", $myJob->RunAs()->Email);


        // Set the ID to null
        $job = new TestQueuedJob();
        $job->JobTitle = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $job->Signature = "Job run as {$job->RunAsID} - " . substr(md5(microtime()), 0, 9);
        $jobId = $svc->queueJob($job, $startAfter = null, $userID = $adminID);

        $list = $svc->getJobList();

        $myJob = $list->byID($jobId);
        // RunAsID is set to current user in QueuedJobService:queueJob()
        $this->assertEquals("ADMIN@example.org", $myJob->RunAs()->Email);
    }

    public function testJobRunAsOnRepeatingJobs()
    {
        $svc = $this->getService();
        $list = $svc->getJobList();
        foreach ($list as $job) {
            $job->delete();
        }

		// Create a new job with no run as ID it will have the current logged in user.
        $adminID = $this->logInWithPermission('ADMIN');
        $job = new RepeatingTestQueuedJob();
        $memberID = Member::currentUserID();
        $job->JobTitle = "Job run as {$memberID} - " . substr(md5(microtime()), 0, 9);
        $adminRunJobID = $svc->queueJob($job);

        // Create a new job with no run as ID it will have the current logged in user.
        Member::currentUser()->logOut();
        $job = new RepeatingTestQueuedJob();
        $memberID = Member::currentUserID();
        $job->JobTitle = "Job run as {$memberID} - " . substr(md5(microtime()), 0, 9);
        $nullRunJobID = $svc->queueJob($job);

        // Log in as another user but set the run ID the Run As User has
        $dummyID = $this->logInWithPermission('DUMMY');
        $job = new RepeatingTestQueuedJob();
        $memberID = Member::currentUserID();
        $job->JobTitle = "Job run as {$memberID} - " . substr(md5(microtime()), 0, 9);
        $dummyRunJobId = $svc->queueJob($job);

        // Set the ID to Admin
        $job = new RepeatingTestQueuedJob();
        $job->JobTitle = "Job run as {$adminID} - " . substr(md5(microtime()), 0, 9);
        $adminIDPushedJobID = $svc->queueJob($job, $startAfter = null, $userID = $adminID);

        $this->assertEquals(4, $svc->getJobList()->count());

        //Log Out to simulate running via command line
        Member::currentUser()->logOut();

        $job = $svc->getNextPendingJob(QueuedJob::QUEUED);
        $svc->runJob($job->ID);
        $this->assertEquals(4, $svc->getJobList()->count());
        $newestJob = $svc->getJobList()->Sort('Created')->Last();
        $this->assertEquals($job->getTitle(), $newestJob->getTitle());
        $this->assertEquals($job->RunAsID, $newestJob->RunAsID);

        $job = $svc->getNextPendingJob(QueuedJob::QUEUED);
        $svc->runJob($job->ID);
        $this->assertEquals(4, $svc->getJobList()->count());
        $newestJob = $svc->getJobList()->First();
        $this->assertEquals($job->getTitle(), $newestJob->getTitle());
        $this->assertEquals($job->RunAsID, $newestJob->RunAsID);

        $job = $svc->getNextPendingJob(QueuedJob::QUEUED);
        $svc->runJob($job->ID);
        $this->assertEquals(4, $svc->getJobList()->count());
        $newestJob = $svc->getJobList()->First();
        $this->assertEquals($job->getTitle(), $newestJob->getTitle());
        $this->assertEquals($job->RunAsID, $newestJob->RunAsID);

        $job = $svc->getNextPendingJob(QueuedJob::QUEUED);
        $svc->runJob($job->ID);
        $this->assertEquals(4, $svc->getJobList()->count());
        $newestJob = $svc->getJobList()->First();
        $this->assertEquals($job->getTitle(), $newestJob->getTitle());
        $this->assertEquals($job->RunAsID, $newestJob->RunAsID);
    }

}

// stub class to be able to call init from an external context
class TestQJService extends QueuedJobService {
	public function testInit($descriptor) {
		return $this->initialiseJob($descriptor);
	}
}

class TestExceptingJob extends  AbstractQueuedJob implements QueuedJob {
    private $type = QueuedJob::QUEUED;

	public function __construct($type = null) {
        $this->type = QueuedJob::IMMEDIATE;
		$this->times = array();
	}

	public function getJobType() {
		return $this->type;
	}

	public function getTitle() {
		return "A Test job";
	}

	public function setup() {
		$this->totalSteps = 1;
	}

	public function process() {
		throw new Exception("just excepted");
	}
}

class TestQueuedJob extends AbstractQueuedJob implements QueuedJob {
	private $type = QueuedJob::QUEUED;

	public function __construct($type = null) {
		if ($type) {
			$this->type = $type;
		}
		$this->times = array();
	}

	public function getJobType() {
		return $this->type;
	}

	public function getTitle() {
		return "A Test job";
	}

	public function setup() {
		$this->totalSteps = 5;
	}

	public function process() {
		$times = $this->times;
		// needed due to quirks with __set
		$times[] = date('Y-m-d H:i:s');
		$this->times = $times;

		$this->addMessage("Updated time to " . date('Y-m-d H:i:s'));
		sleep(1);

		// make sure we're incrementing
		$this->currentStep++;

		// and checking whether we're complete
		if ($this->currentStep == 5) {
			$this->isComplete = true;
		}
	}
}

class RepeatingTestQueuedJob extends TestQueuedJob {

    public function __construct($type = null)
    {
        parent::__construct($type);
        $this->Signature = $this->JobTitle;
    }

    public function getTitle()
    {
        return $this->JobTitle;
    }

    public function afterComplete()
    {
        $nextJob = new RepeatingTestQueuedJob();
        $nextJob->JobTitle = $this->JobTitle;
        singleton('TestQJService')->queueJob($nextJob, date('Y-m-d H:i:s', time()));
    }
}
