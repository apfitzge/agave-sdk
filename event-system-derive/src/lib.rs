#![cfg_attr(docsrs, feature(doc_cfg))]

use {
    proc_macro::TokenStream,
    proc_macro_crate::{FoundCrate, crate_name},
    proc_macro2::Span,
    quote::{format_ident, quote},
    syn::{DeriveInput, LitStr, parse::Parser, parse_macro_input},
};

/// Implements `agave_event_system::Event` for a struct or enum.
/// Mark a borrowed byte slice with `#[payload]` to store its bytes separately
/// from the fixed-size cell. Its wire fields are `<name>_offset` and `<name>_len`.
/// Other dynamically sized fields require `max_serialized_size`.
#[proc_macro_attribute]
pub fn event(args: TokenStream, item: TokenStream) -> TokenStream {
    event_impl(args.into(), parse_macro_input!(item as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn event_impl(
    args: proc_macro2::TokenStream,
    mut input: DeriveInput,
) -> syn::Result<proc_macro2::TokenStream> {
    if input.generics.type_params().next().is_some()
        || input.generics.const_params().next().is_some()
    {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Event supports lifetime parameters only; implement generic events by hand",
        ));
    }
    let EventArgs {
        max_serialized_size,
    } = event_args(args)?;
    let krate = event_system_crate()?;
    let macro_path = LitStr::new(
        &quote!(#krate::__private::event_macro).to_string(),
        Span::call_site(),
    );
    let ident = input.ident.clone();
    let wire_ident = format_ident!("__EventWire{}", ident);
    let mut wire = input.clone();
    wire.ident = wire_ident.clone();
    wire.vis = syn::Visibility::Inherited;
    wire.generics = syn::Generics::default();
    wire.attrs.retain(|attr| attr.path().is_ident("wincode"));
    let mut descriptors = Vec::new();
    let mut payload_arms = Vec::new();
    let mut decode_arms = Vec::new();
    match (&mut input.data, &mut wire.data) {
        (syn::Data::Struct(data), syn::Data::Struct(wire)) => {
            let generated = payload_fields(
                &mut data.fields,
                &mut wire.fields,
                None,
                &ident,
                &wire_ident,
                &krate,
            )?;
            descriptors.extend(generated.descriptor);
            payload_arms.push(generated.payload);
            decode_arms.push(generated.decode);
        }
        (syn::Data::Enum(data), syn::Data::Enum(wire)) => {
            for (variant, wire) in data.variants.iter_mut().zip(&mut wire.variants) {
                let generated = payload_fields(
                    &mut variant.fields,
                    &mut wire.fields,
                    Some(&variant.ident),
                    &ident,
                    &wire_ident,
                    &krate,
                )?;
                let cfg: Vec<_> = variant
                    .attrs
                    .iter()
                    .filter(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
                    .collect();
                if let Some(descriptor) = generated.descriptor {
                    descriptors.push(quote!(#(#cfg)* #descriptor));
                }
                let payload = generated.payload;
                let decode = generated.decode;
                payload_arms.push(quote!(#(#cfg)* #payload));
                decode_arms.push(quote!(#(#cfg)* #decode));
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "Event requires a struct or enum",
            ));
        }
    }
    let has_payload = !descriptors.is_empty();
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let lifetimes: Vec<_> = input
        .generics
        .lifetimes()
        .map(|_| quote!('__event_view))
        .collect();
    let view = if lifetimes.is_empty() {
        quote!(#ident)
    } else {
        quote!(#ident<#(#lifetimes),*>)
    };
    let max_size = max_serialized_size.map_or_else(|| quote!(None), |n| quote!(Some(#n)));
    let (extra, derives, methods) = if has_payload {
        (
            quote! {
                #[derive(#krate::wincode::SchemaRead, #krate::wincode::SchemaWrite,
                    #krate::wincode_dynamic::SchemaDynamic)]
                #[wincode(crate = #macro_path)]
                #wire
                impl #impl_generics #krate::wincode_dynamic::SchemaDynamic for #ident #type_generics #where_clause {
                    const SERIALIZED_SIZE: #krate::wincode_dynamic::SerializedSize =
                        <#wire_ident as #krate::wincode_dynamic::SchemaDynamic>::SERIALIZED_SIZE;
                    fn schema() -> #krate::wincode_dynamic::RootSchema {
                        use #krate::wincode_dynamic::{RootSchema, Schema, SchemaDynamic};
                        match #wire_ident::schema() {
                            RootSchema::Struct(s) => RootSchema::Struct(Schema::new(
                                stringify!(#ident), s.field_defs().to_vec().into_boxed_slice(), s.size())),
                            RootSchema::Enum { variants, size, tag_encoding, .. } => RootSchema::Enum {
                                variants, size, tag_encoding, name: stringify!(#ident).into(),
                            },
                        }
                    }
                }
            },
            quote!(#[derive(#krate::wincode::SchemaWrite)]),
            quote! {
                const HAS_PAYLOAD: bool = !Self::PAYLOAD_FIELDS.is_empty();
                const PAYLOAD_FIELDS: &'static [(Option<&'static str>, &'static str)] = &[#(#descriptors),*];
                fn payload_data(&self) -> Option<&[u8]> { match self { #(#payload_arms),* } }
                fn decode_event<'__event_view>(header: &[u8], __event_payload: &'__event_view [u8])
                    -> #krate::wincode::ReadResult<Self::View<'__event_view>> {
                    let wire: #wire_ident = #krate::wincode::deserialize(header)?;
                    Ok(match wire { #(#decode_arms),* })
                }
            },
        )
    } else {
        if !input.generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &input.generics,
                "event lifetimes must belong to #[payload] byte slices",
            ));
        }
        (
            quote!(),
            quote!(#[derive(#krate::wincode::SchemaRead,
            #krate::wincode::SchemaWrite, #krate::wincode_dynamic::SchemaDynamic)]),
            quote! {
                fn decode_event<'__event_view>(header: &[u8], _: &'__event_view [u8])
                    -> #krate::wincode::ReadResult<Self::View<'__event_view>> {
                    #krate::wincode::deserialize(header)
                }
            },
        )
    };
    let wire_size_type = if has_payload {
        quote!(#wire_ident)
    } else {
        quote!(#ident)
    };
    Ok(quote! {
        #derives
        #[wincode(crate = #macro_path)]
        #input
        const _: () = {
            #extra
            // SAFETY: byte-array cells contain no padding; all lifetimes share the
            // same wire schema. Only generated offset fields designate payloads.
            unsafe impl #impl_generics #krate::Event for #ident #type_generics #where_clause {
                type View<'__event_view> = #view;
                type QueueCell = [u8; #krate::event_queue_cell_size(
                    <#wire_size_type as #krate::wincode_dynamic::SchemaDynamic>::SERIALIZED_SIZE, #max_size)];
                #methods
            }
        };
    })
}

struct PayloadFields {
    descriptor: Option<proc_macro2::TokenStream>,
    payload: proc_macro2::TokenStream,
    decode: proc_macro2::TokenStream,
}

fn payload_fields(
    fields: &mut syn::Fields,
    wire: &mut syn::Fields,
    variant: Option<&syn::Ident>,
    ident: &syn::Ident,
    wire_ident: &syn::Ident,
    krate: &proc_macro2::TokenStream,
) -> syn::Result<PayloadFields> {
    let mut marked = None;
    let original_names: Vec<_> = fields.iter().filter_map(|f| f.ident.clone()).collect();
    let mut wire_fields = syn::punctuated::Punctuated::new();
    let mut read_bindings = Vec::new();
    let mut values = Vec::new();
    let mut payload_bindings = Vec::new();
    let mut descriptor = None;
    for (index, field) in fields.iter_mut().enumerate() {
        let markers: Vec<_> = field
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("payload"))
            .collect();
        let binding = format_ident!("__field_{}", index);
        let member = field.ident.as_ref().map(|name| quote!(#name:));
        if markers.is_empty() {
            wire_fields.push(field.clone());
            read_bindings.push(quote!(#member #binding));
            values.push(quote!(#member #binding));
            payload_bindings.push(quote!(#member _));
            continue;
        }
        if markers.len() != 1 || !matches!(markers[0].meta, syn::Meta::Path(_)) {
            return Err(syn::Error::new_spanned(
                field,
                "use one #[payload] marker without arguments",
            ));
        }
        if marked.replace(index).is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "only one #[payload] field per struct or variant is supported",
            ));
        }
        let syn::Type::Reference(reference) = &field.ty else {
            return Err(syn::Error::new_spanned(
                field,
                "#[payload] requires &'a [u8]",
            ));
        };
        let is_bytes = matches!(&*reference.elem, syn::Type::Slice(slice)
            if matches!(&*slice.elem, syn::Type::Path(ty) if ty.path.is_ident("u8")));
        if reference.mutability.is_some() || reference.lifetime.is_none() || !is_bytes {
            return Err(syn::Error::new_spanned(
                field,
                "#[payload] requires an immutable &'a [u8]",
            ));
        }
        let lifetime = &reference.lifetime;
        let adapter = LitStr::new(
            &quote!(#krate::__private::PayloadSlice<#lifetime>).to_string(),
            Span::call_site(),
        );
        field.attrs.retain(|a| !a.path().is_ident("payload"));
        if field.attrs.iter().any(|a| {
            a.path().is_ident("wincode")
                || a.path().is_ident("cfg")
                || a.path().is_ident("cfg_attr")
        }) {
            return Err(syn::Error::new_spanned(
                field,
                "#[payload] controls its wire encoding; put cfg on the variant or event",
            ));
        }
        for suffix in ["offset", "len"] {
            let mut numeric = field.clone();
            numeric.ty = syn::parse_quote!(u64);
            numeric.ident = field
                .ident
                .as_ref()
                .map(|name| format_ident!("{}_{}", name, suffix));
            if numeric
                .ident
                .as_ref()
                .is_some_and(|name| original_names.contains(name))
            {
                return Err(syn::Error::new_spanned(
                    field,
                    "generated payload offset/length field conflicts with an existing field",
                ));
            }
            if suffix == "offset" {
                let name = numeric
                    .ident
                    .as_ref()
                    .map_or_else(|| index.to_string(), ToString::to_string);
                let variant_name = variant.map_or_else(
                    || quote!(None),
                    |v| {
                        let v = v.to_string();
                        quote!(Some(#v))
                    },
                );
                descriptor = Some(quote!((#variant_name, #name)));
            }
            let numeric_member = numeric.ident.as_ref().map(|name| quote!(#name:));
            read_bindings.push(quote!(#numeric_member _));
            wire_fields.push(numeric);
        }
        field
            .attrs
            .push(syn::parse_quote!(#[wincode(with = #adapter)]));
        payload_bindings.push(quote!(#member __event_payload));
        values.push(quote!(#member __event_payload));
    }
    let source_path = variant.map_or_else(|| quote!(#ident), |v| quote!(#ident::#v));
    let wire_path = variant.map_or_else(|| quote!(#wire_ident), |v| quote!(#wire_ident::#v));
    let wrap = |path, parts: Vec<proc_macro2::TokenStream>| match fields {
        syn::Fields::Named(_) => quote!(#path { #(#parts),* }),
        syn::Fields::Unnamed(_) => quote!(#path(#(#parts),*)),
        syn::Fields::Unit => quote!(#path),
    };
    let pattern = wrap(source_path.clone(), payload_bindings);
    let read_pattern = wrap(wire_path, read_bindings);
    let value = wrap(source_path, values);
    match wire {
        syn::Fields::Named(fields) => fields.named = wire_fields,
        syn::Fields::Unnamed(fields) => fields.unnamed = wire_fields,
        syn::Fields::Unit => (),
    }
    let payload = if marked.is_some() {
        quote!(Some(*__event_payload))
    } else {
        quote!(None)
    };
    Ok(PayloadFields {
        descriptor,
        payload: quote!(#pattern => #payload),
        decode: quote!(#read_pattern => #value),
    })
}

fn event_system_crate() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("agave-event-system") {
        Ok(FoundCrate::Itself) => Ok(quote!(::agave_event_system)),
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name.replace('-', "_"));
            Ok(quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(Span::call_site(), error)),
    }
}

struct EventArgs {
    max_serialized_size: Option<usize>,
}

fn event_args(args: proc_macro2::TokenStream) -> syn::Result<EventArgs> {
    let mut max_serialized_size = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("max_serialized_size") {
            if max_serialized_size.is_some() {
                return Err(meta.error("duplicate `max_serialized_size`"));
            }
            let value = meta.value()?.parse::<syn::LitInt>()?;
            max_serialized_size = Some(value.base10_parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported or duplicate event attribute"))
        }
    });
    parser.parse2(args)?;
    Ok(EventArgs {
        max_serialized_size,
    })
}
