/* 
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.zeromq;

import java.util.List;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import org.apache.flink.zeromq.common.ZMQConnectionConfig;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZeroMQ source (consumer) which reads from a queue.
 *
 * @param <OUT> The type of the data read from ZeroMQ.
 */
public class ZMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long>
	implements ResultTypeQueryable<OUT> {

	private static final long serialVersionUID = -2507921066985182520L;
	
	private static final Logger LOG = LoggerFactory.getLogger(ZMQSource.class);

	private final ZMQConnectionConfig zmqConnectionConfig;
	private final String queueName;
	private final boolean usesCorrelationId;
	protected DeserializationSchema<OUT> schema;

	protected transient boolean autoAck;
	protected transient Context context;
	protected transient Socket frontend;

	private transient volatile boolean running;
	
	public ZMQSource(ZMQConnectionConfig zmqConnectionConfig, String queueName,
					DeserializationSchema<OUT> deserializationSchema) {
		this(zmqConnectionConfig, queueName, false, deserializationSchema);
	}

	protected ZMQSource(ZMQConnectionConfig zmqConnectionConfig, String queueName,
						boolean usesCorrelationId, DeserializationSchema<OUT> deserializationSchema) {
		super(String.class);
		this.zmqConnectionConfig = zmqConnectionConfig;
		this.queueName = queueName;
		this.usesCorrelationId = usesCorrelationId;
		this.schema = deserializationSchema;
	}
	
	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		
		//  Prepare our context and sockets
		context = ZMQ.context(1);

		//  Socket facing clients
		frontend = context.socket(ZMQ.PULL);
		frontend.bind(zmqConnectionConfig.getUri());

		RuntimeContext runtimeContext = getRuntimeContext();
		if (runtimeContext instanceof StreamingRuntimeContext
				&& ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
			autoAck = false;
			// enables transaction mode
			//channel.txSelect();
		} else {
			autoAck = true;
		}

		LOG.debug("Starting ZeroMQ source with autoAck status: " + autoAck);
		LOG.debug("Starting ZeroMQ source with uri: " + zmqConnectionConfig.getUri());
		//channel.basicConsume(queueName, autoAck, consumer);
		running = true;
	}

	public void run(SourceContext<OUT> ctx) throws Exception {
		while (running) {
			//TODO Logic here
			//  Wait for start of batch
			String string = new String(frontend.recv(0)).trim();
			//TODO Use deserializer
			OUT result = (OUT) string;
			//OUT result = schema.deserialize(delivery.getBody());
			ctx.collect(result);
		}
	}

	public void cancel() {
		//TODO Complete cancel
		running = false;
	}

	public TypeInformation<OUT> getProducedType() {
		return schema.getProducedType();
	}

	@Override
	protected void acknowledgeSessionIDs(List<Long> sessionIds) {
		//TODO Acknowledge msgs
		/*try {
			for (long id : sessionIds) {
				channel.basicAck(id, false);
			}
			channel.txCommit();
		} catch (IOException e) {
			throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
		}*/
	}
	
}
