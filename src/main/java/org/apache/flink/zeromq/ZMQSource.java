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

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.zeromq.common.ZMQConnectionConfig;
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

	//protected transient Connection connection;
	//protected transient Channel channel;
	//protected transient QueueingConsumer consumer;

	protected transient boolean autoAck;

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

	public void run(SourceContext<OUT> ctx) throws Exception {
		while (running) {
			//TODO Logic here
		}
	}

	public void cancel() {
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
