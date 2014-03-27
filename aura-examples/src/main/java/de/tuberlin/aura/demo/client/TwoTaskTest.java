package de.tuberlin.aura.demo.client;

public final class TwoTaskTest {
    /*
     * private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TwoTaskTest.class);
     * 
     * // Disallow Instantiation. private TwoTaskTest() {
     * 
     * }
     * 
     * public static class Task1Exe extends TaskInvokeable {
     * 
     * public Task1Exe(final TaskRuntimeContext context, final Logger LOG) { super(context, LOG); }
     * 
     * @Override public void execute() throws Exception {
     * 
     * final UUID taskID = getTaskID();
     * 
     * for (int i = 0; i < 10000; ++i) {
     * 
     * final List<Descriptors.TaskDescriptor> outputs =
     * context.taskBinding.outputGateBindings.get(0); for (int index = 0; index < outputs.size();
     * ++index) { final UUID outputTaskID = getOutputTaskID(0, index);
     * 
     * ByteBuffer buffer = ByteBuffer.allocate(64 << 10); buffer.putInt(i); buffer.flip();
     * 
     * final DataIOEvent outputBuffer = new TransferBufferEvent(taskID, outputTaskID,
     * buffer.array()); emit(0, index, outputBuffer); } }
     * 
     * final List<Descriptors.TaskDescriptor> outputs =
     * context.taskBinding.outputGateBindings.get(0); for (int index = 0; index < outputs.size();
     * ++index) { final UUID outputTaskID = getOutputTaskID(0, index); final DataIOEvent
     * exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID,
     * outputTaskID); emit(0, index, exhaustedEvent); } } }
     * 
     * public static class Task2Exe extends TaskInvokeable {
     * 
     * int count = 0;
     * 
     * int sum_received = 0;
     * 
     * int sum_count = 0;
     * 
     * public Task2Exe(final TaskRuntimeContext context, final Logger LOG) { super(context, LOG); }
     * 
     * @Override public void execute() throws Exception {
     * 
     * openGate(0);
     * 
     * while (isTaskRunning()) {
     * 
     * final DataIOEvent inputBuffer = absorb(0);
     * 
     * if (inputBuffer != null) {
     * 
     * int received = ByteBuffer.wrap(((TransferBufferEvent) inputBuffer).data).getInt(); //
     * LOG.error("- received: " + received + " - count: " + count);
     * 
     * 
     * sum_received += received; sum_count += count;
     * 
     * count++; }
     * 
     * checkIfSuspended(); }
     * 
     * LOG.error("received sum: " + sum_received + " -- count sum: " + sum_count);
     * 
     * LOG.info("RECEIVED ELEMENTS: " + count); } }
     * 
     * // --------------------------------------------------- // Main. //
     * ---------------------------------------------------
     * 
     * public static void main(String[] args) {
     * 
     * final String zookeeperAddress = "localhost:2181"; final LocalClusterSimulator lce = new
     * LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
     * true, zookeeperAddress, 6); final AuraClient ac = new AuraClient(zookeeperAddress, 25340,
     * 26340);
     * 
     * // final String zookeeperAddress = "wally100.cit.tu-berlin.de:2181"; // final AuraClient ac =
     * new AuraClient(zookeeperAddress, 10000, 11111);
     * 
     * final AuraTopologyBuilder atb1 = ac.createTopologyBuilder(); atb1.addNode(new
     * Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class) // .connectTo("Task33",
     * Edge.TransferType.ALL_TO_ALL) // .addNode(new Node(UUID.randomUUID(), "Task33", 5, 1),
     * Task33Exe.class) .connectTo("Task2", Edge.TransferType.POINT_TO_POINT) .addNode(new
     * Node(UUID.randomUUID(), "Task2", 1, 1), Task2Exe.class);
     * 
     * 
     * final AuraTopology at1 = atb1.build("Job 1",
     * EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
     * 
     * ac.submitTopology(at1, null);
     * 
     * try { new BufferedReader(new InputStreamReader(System.in)).readLine(); } catch (IOException
     * e) { e.printStackTrace(); }
     * 
     * lce.shutdown(); }
     */
}
