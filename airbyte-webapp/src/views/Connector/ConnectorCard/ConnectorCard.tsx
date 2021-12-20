import React from "react";

import { ContentCard } from "components";
import { JobsLogItem } from "components/JobItem";
import ServiceForm from "views/Connector/ServiceForm";
import { ServiceFormProps } from "views/Connector/ServiceForm/types";

const ConnectorCard: React.FC<
  { title?: React.ReactNode; full?: boolean; jobInfo: any } & ServiceFormProps
> = ({ title, full, jobInfo, ...props }) => {
  return (
    <ContentCard title={title} full={full}>
      <ServiceForm {...props} />
      <JobsLogItem jobInfo={jobInfo} />
    </ContentCard>
  );
};

export { ConnectorCard };
