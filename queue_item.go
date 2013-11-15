package kestrel

type QueueItem struct {
	*Item

	queueName string
	client    *Client
}

func NewQueueItem(item *Item, queueName string, client *Client) *QueueItem {
	return &QueueItem{item, queueName, client}
}

func NewQueueItems(items []*Item, queueName string, client *Client) []*QueueItem {
	queueItems := make([]*QueueItem, len(items))
	for i, item := range items {
		queueItems[i] = NewQueueItem(item, queueName, client)
	}

	return queueItems
}

func (q *QueueItem) Confirm() error {
	_, err := q.client.Confirm(q.queueName, []*QueueItem{q})
	return err
}

func (q *QueueItem) Abort() error {
	_, err := q.client.Abort(q.queueName, []*QueueItem{q})
	return err
}
