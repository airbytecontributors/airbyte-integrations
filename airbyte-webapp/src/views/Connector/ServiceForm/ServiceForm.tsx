import React, { useCallback, useEffect, useMemo } from "react";
import {
  Formik,
  FormikErrors,
  isFunction,
  setIn,
  useFormikContext,
  validateYupSchema,
  yupToFormErrors,
} from "formik";
import { JSONSchema7 } from "json-schema";
import { useToggle } from "react-use";

import {
  useBuildForm,
  useBuildInitialSchema,
  useBuildUiWidgetsContext,
  useConstructValidationSchema,
  usePatchFormik,
} from "./useBuildForm";
import { ServiceFormValues } from "./types";
import {
  ServiceFormContextProvider,
  useServiceForm,
} from "./serviceFormContext";
import { FormRoot } from "./FormRoot";
import RequestConnectorModal from "views/Connector/RequestConnectorModal";
import { FormBaseItem } from "core/form/types";
import { ConnectorNameControl } from "./components/Controls/ConnectorNameControl";
import { ConnectorServiceTypeControl } from "./components/Controls/ConnectorServiceTypeControl";
import {
  ConnectorDefinition,
  ConnectorDefinitionSpecification,
} from "core/domain/connector";
import { isDefined } from "utils/common";

type ServiceFormProps = {
  formType: "source" | "destination";
  availableServices: ConnectorDefinition[];
  selectedConnector?: ConnectorDefinitionSpecification;
  onServiceSelect?: (id: string) => void;
  onSubmit: (values: ServiceFormValues) => void;
  onRetest?: (values: ServiceFormValues) => void;
  isLoading?: boolean;
  isEditMode?: boolean;
  allowChangeConnector?: boolean;
  formValues?: Partial<ServiceFormValues>;
  hasSuccess?: boolean;
  additionBottomControls?: React.ReactNode;
  fetchingConnectorError?: Error;
  errorMessage?: React.ReactNode;
  successMessage?: React.ReactNode;
};

const FormikPatch: React.FC = () => {
  usePatchFormik();
  return null;
};

/***
 * This function sets all initial const values in the form to current values
 * @param schema
 * @constructor
 */
const PatchInitialValuesWithWidgetConfig: React.FC<{ schema: JSONSchema7 }> = ({
  schema,
}) => {
  const { widgetsInfo } = useServiceForm();
  const { values, setValues } = useFormikContext();

  const formInitialValues = useMemo(() => {
    return Object.entries(widgetsInfo)
      .filter(([_, v]) => isDefined(v.const))
      .reduce((acc, [k, v]) => setIn(acc, k, v.const), values);
  }, [schema]);

  useEffect(() => setValues(formInitialValues), [formInitialValues]);

  return null;
};

const useValidationSchemaToValidate = (validationSchema: any, context: any) =>
  React.useCallback(
    (values: any, field?: string): Promise<FormikErrors<any>> => {
      const schema = isFunction(validationSchema)
        ? validationSchema(field)
        : validationSchema;
      const promise =
        field && schema.validateAt
          ? schema.validateAt(field, values)
          : validateYupSchema(values, schema, undefined, context);
      return new Promise((resolve, reject) => {
        promise.then(
          () => {
            resolve({});
          },
          (err: any) => {
            // Yup will throw a validation error if validation fails. We catch those and
            // resolve them into Formik errors. We can sniff if something is a Yup error
            // by checking error.name.
            // @see https://github.com/jquense/yup#validationerrorerrors-string--arraystring-value-any-path-string
            if (err.name === "ValidationError") {
              resolve(yupToFormErrors(err));
            } else {
              // We throw any other errors
              if (process.env.NODE_ENV !== "production") {
                console.warn(
                  `Warning: An unhandled error was caught during validation in <Formik validationSchema />`,
                  err
                );
              }

              reject(err);
            }
          }
        );
      });
    },
    [validationSchema, context]
  );

const ServiceForm: React.FC<ServiceFormProps> = (props) => {
  const [isOpenRequestModal, toggleOpenRequestModal] = useToggle(false);
  const {
    formType,
    formValues,
    onSubmit,
    isLoading,
    selectedConnector,
    onRetest,
  } = props;

  const jsonSchema = useBuildInitialSchema(selectedConnector, isLoading);
  const { formFields, initialValues } = useBuildForm(jsonSchema, formValues);

  const uiOverrides = useMemo(
    () => ({
      name: {
        component: (property: FormBaseItem) => (
          <ConnectorNameControl property={property} formType={formType} />
        ),
      },
      serviceType: {
        component: (property: FormBaseItem) => (
          <ConnectorServiceTypeControl
            property={property}
            formType={formType}
            documentationUrl={selectedConnector?.documentationUrl}
            onChangeServiceType={props.onServiceSelect}
            availableServices={props.availableServices}
            allowChangeConnector={props.allowChangeConnector}
            isEditMode={props.isEditMode}
            onOpenRequestConnectorModal={toggleOpenRequestModal}
          />
        ),
      },
    }),
    [
      formType,
      toggleOpenRequestModal,
      props.allowChangeConnector,
      props.availableServices,
      props.selectedConnector,
      props.isEditMode,
      props.onServiceSelect,
    ]
  );

  const { uiWidgetsInfo, setUiWidgetsInfo } = useBuildUiWidgetsContext(
    formFields,
    initialValues,
    uiOverrides
  );

  const validationSchema = useConstructValidationSchema(
    uiWidgetsInfo,
    jsonSchema
  );

  const onFormSubmit = useCallback(
    async (values) => {
      const valuesToSend = validationSchema.cast(values, {
        stripUnknown: true,
      });
      return onSubmit(valuesToSend);
    },
    [onSubmit, validationSchema]
  );

  const onRetestForm = useCallback(
    async (values) => {
      if (!onRetest) {
        return null;
      }
      const valuesToSend = validationSchema.cast(values, {
        stripUnknown: true,
      });
      return onRetest(valuesToSend);
    },
    [onRetest, validationSchema]
  );

  const runValidationSchema = useValidationSchemaToValidate(
    validationSchema,
    uiWidgetsInfo
  );

  return (
    <Formik
      validateOnBlur={true}
      validateOnChange={true}
      initialValues={initialValues}
      // validationSchema={validationSchema}
      validate={runValidationSchema}
      onSubmit={onFormSubmit}
    >
      {({ values, setSubmitting }) => (
        <ServiceFormContextProvider
          widgetsInfo={uiWidgetsInfo}
          setUiWidgetsInfo={setUiWidgetsInfo}
          formType={formType}
          selectedConnector={selectedConnector}
          availableServices={props.availableServices}
          isEditMode={props.isEditMode}
          isLoadingSchema={props.isLoading}
        >
          <FormikPatch />
          <PatchInitialValuesWithWidgetConfig schema={jsonSchema} />
          <FormRoot
            {...props}
            onRetest={async () => {
              setSubmitting(true);
              await onRetestForm(values);
              setSubmitting(false);
            }}
            formFields={formFields}
          />
          {isOpenRequestModal && (
            <RequestConnectorModal
              connectorType={formType}
              onClose={toggleOpenRequestModal}
            />
          )}
        </ServiceFormContextProvider>
      )}
    </Formik>
  );
};
export default ServiceForm;
